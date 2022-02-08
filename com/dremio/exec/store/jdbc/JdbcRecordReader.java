package com.dremio.exec.store.jdbc;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.FieldSizeLimitExceptionHelper;
import com.dremio.common.exceptions.InvalidMetadataErrorContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.BasePath;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.exec.store.StoragePluginUtils;
import com.dremio.exec.store.jdbc.dialect.JdbcToFieldMapping;
import com.dremio.exec.store.jdbc.dialect.TypeMapper;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@VisibleForTesting
public final class JdbcRecordReader extends AbstractRecordReader {
   private static final boolean DISABLE_TRIM_ON_CHARS = Boolean.getBoolean("dremio.jdbc.mssql.trim-on-chars.disable");
   private static final Logger logger = LoggerFactory.getLogger(JdbcRecordReader.class);
   private final DataSource source;
   private ResultSet resultSet;
   private final String storagePluginName;
   private Connection connection;
   private volatile Statement statement;
   private final String sql;
   private ImmutableList<ValueVector> vectors;
   private ImmutableList<JdbcRecordReader.Copier<?>> copiers;
   private boolean hasNext = true;
   private final List<SchemaPath> columns;
   private final int fetchSize;
   private final AtomicBoolean cancelled;
   private final boolean needsTrailingPaddingTrim;
   private final boolean coerceTimesToUTC;
   private final boolean coerceTimestampsToUTC;
   private final boolean adjustDateTimezone;
   private final TypeMapper dialectTypeMapper;
   private final Collection<List<String>> tableList;
   private final Set<String> skippedColumns;
   private final int maxCellSize;
   private final int timeoutSeconds;
   private static final String CANCEL_ERROR_MESSAGE = "Call to cancel query on RDBMs source failed with error: {}";

   public JdbcRecordReader(OperatorContext context, DataSource source, String sql, JdbcPluginConfig config, List<SchemaPath> columns, ListenableFuture<Boolean> cancelled, SourceCapabilities capabilities, TypeMapper typeMapper, Collection<List<String>> tableList, Set<String> skippedColumns) {
      super(context, columns);
      this.source = source;
      this.sql = sql;
      this.storagePluginName = config.getSourceName();
      this.columns = columns;
      this.fetchSize = config.getFetchSize();
      this.timeoutSeconds = config.getQueryTimeout();
      this.needsTrailingPaddingTrim = capabilities.getCapability(JdbcStoragePlugin.REQUIRE_TRIMS_ON_CHARS);
      this.coerceTimesToUTC = capabilities.getCapability(JdbcStoragePlugin.COERCE_TIMES_TO_UTC);
      this.coerceTimestampsToUTC = capabilities.getCapability(JdbcStoragePlugin.COERCE_TIMESTAMPS_TO_UTC);
      this.adjustDateTimezone = capabilities.getCapability(JdbcStoragePlugin.ADJUST_DATE_TIMEZONE);
      this.dialectTypeMapper = typeMapper;
      this.tableList = tableList;
      this.skippedColumns = skippedColumns;
      this.cancelled = new AtomicBoolean(false);
      if (context != null) {
         this.maxCellSize = Math.toIntExact(context.getOptions().getOption(ExecConstants.LIMIT_FIELD_SIZE_BYTES));
         Futures.addCallback(cancelled, new FutureCallback<Boolean>() {
            public void onFailure(Throwable t) {
            }

            public void onSuccess(Boolean result) {
               JdbcRecordReader.this.cancel();
            }
         }, context.getExecutor());
      } else {
         this.maxCellSize = Math.toIntExact(ExecConstants.LIMIT_FIELD_SIZE_BYTES.getDefault().getNumVal());
      }

   }

   private JdbcRecordReader.Copier<?> getCopier(int offset, ResultSet result, ValueVector v) {
      if (v instanceof BigIntVector) {
         return new JdbcRecordReader.BigIntCopier(offset, result, (BigIntVector)v);
      } else if (v instanceof Float4Vector) {
         return new JdbcRecordReader.Float4Copier(offset, result, (Float4Vector)v);
      } else if (v instanceof Float8Vector) {
         return new JdbcRecordReader.Float8Copier(offset, result, (Float8Vector)v);
      } else if (v instanceof IntVector) {
         return new JdbcRecordReader.IntCopier(offset, result, (IntVector)v);
      } else if (v instanceof VarCharVector) {
         return (JdbcRecordReader.Copier)(!DISABLE_TRIM_ON_CHARS && this.needsTrailingPaddingTrim ? new JdbcRecordReader.VarCharTrimCopier(this.maxCellSize, offset, result, (VarCharVector)v) : new JdbcRecordReader.VarCharCopier(this.maxCellSize, offset, result, (VarCharVector)v));
      } else if (v instanceof VarBinaryVector) {
         return new JdbcRecordReader.VarBinaryCopier(this.maxCellSize, offset, result, (VarBinaryVector)v);
      } else if (v instanceof DateMilliVector) {
         return (JdbcRecordReader.Copier)(this.adjustDateTimezone ? new JdbcRecordReader.DateTimeZoneAdjustmentCopier(offset, result, (DateMilliVector)v) : new JdbcRecordReader.DateCopier(offset, result, (DateMilliVector)v));
      } else if (v instanceof TimeMilliVector) {
         return (JdbcRecordReader.Copier)(this.coerceTimesToUTC ? new JdbcRecordReader.TimeCopierCoerceToUTC(offset, result, (TimeMilliVector)v) : new JdbcRecordReader.TimeCopier(offset, result, (TimeMilliVector)v));
      } else if (v instanceof TimeStampMilliVector) {
         return (JdbcRecordReader.Copier)(this.coerceTimestampsToUTC ? new JdbcRecordReader.TimeStampCopierCoerceToUTC(offset, result, (TimeStampMilliVector)v) : new JdbcRecordReader.TimeStampCopier(offset, result, (TimeStampMilliVector)v));
      } else if (v instanceof BitVector) {
         return new JdbcRecordReader.BitCopier(offset, result, (BitVector)v);
      } else if (v instanceof DecimalVector) {
         return new JdbcRecordReader.DecimalCopier(offset, result, (DecimalVector)v);
      } else {
         throw new IllegalArgumentException("Unknown how to handle vector.");
      }
   }

   public void setup(OutputMutator output) {
      long longFetchSize = this.fetchSize != 0 ? (long)Math.min(this.fetchSize, this.numRowsPerBatch) : (long)this.numRowsPerBatch;
      int fetchSize = Ints.saturatedCast(longFetchSize);

      try {
         this.connection = this.source.getConnection();
         if (null != this.context) {
            try {
               this.connection.setNetworkTimeout(this.context.getExecutor(), Ints.saturatedCast(TimeUnit.SECONDS.toMillis((long)this.timeoutSeconds)));
            } catch (Throwable var9) {
            }
         }

         this.statement = this.connection.createStatement();
         if (this.cancelled.get()) {
            throw StoragePluginUtils.message(UserException.ioExceptionError(new CancellationException()), this.storagePluginName, "Query was cancelled.", new Object[0]).addContext("sql", this.sql).build(logger);
         } else {
            this.statement.setFetchSize(fetchSize);
            this.statement.setQueryTimeout(this.timeoutSeconds);
            this.resultSet = this.statement.executeQuery(this.sql);
            ResultSetMetaData meta = this.resultSet.getMetaData();
            Builder<ValueVector> vectorBuilder = ImmutableList.builder();
            Builder<JdbcRecordReader.Copier<?>> copierBuilder = ImmutableList.builder();
            List<JdbcToFieldMapping> mappings = this.dialectTypeMapper.mapJdbcToArrowFields((TypeMapper.UnrecognizedTypeMarker)null, (TypeMapper.AddPropertyCallback)null, (TypeMapper.InvalidMetaDataCallback)(this::throwInvalidMetadataError), (Connection)this.connection, (ResultSetMetaData)meta, (Set)this.skippedColumns, false);
            mappings.forEach((mapping) -> {
               if (!this.skippedColumns.contains(mapping.getField().getName().toLowerCase(Locale.ROOT))) {
                  this.addFieldToOutput(output, vectorBuilder, copierBuilder, mapping);
               }

            });
            this.checkSchemaConsistency(mappings);
            this.vectors = vectorBuilder.build();
            this.copiers = copierBuilder.build();
         }
      } catch (SQLException var10) {
         throw StoragePluginUtils.message(UserException.dataReadError(var10), this.storagePluginName, var10.getMessage(), new Object[0]).addContext("sql", this.sql).build(logger);
      } catch (SchemaChangeException var11) {
         throw UserException.dataReadError(var11).message("The JDBC storage plugin failed while trying setup the SQL query. ", new Object[0]).addContext("sql", this.sql).addContext("plugin", this.storagePluginName).build(logger);
      }
   }

   public int next() {
      int counter = 0;

      try {
         while(counter < this.numRowsPerBatch && this.hasNext) {
            this.hasNext = this.resultSet.next();
            if (!this.hasNext) {
               break;
            }

            UnmodifiableIterator var2 = this.copiers.iterator();

            while(var2.hasNext()) {
               JdbcRecordReader.Copier<?> c = (JdbcRecordReader.Copier)var2.next();
               c.copy(counter);
            }

            ++counter;
         }
      } catch (SQLException var4) {
         throw StoragePluginUtils.message(UserException.dataReadError(var4), this.storagePluginName, var4.getMessage(), new Object[0]).addContext("sql", this.sql).build(logger);
      } catch (IllegalArgumentException var5) {
         throw UserException.validationError(var5).message(var5.getMessage(), new Object[0]).addContext("sql", this.sql).build();
      }

      this.vectors.forEach((vv) -> {
         vv.setValueCount(counter);
      });
      return counter;
   }

   public void cancel() {
      boolean alreadyCancelled = this.cancelled.getAndSet(true);
      if (!alreadyCancelled) {
         try {
            if (this.statement != null && !this.statement.isClosed()) {
               this.statement.cancel();
            }
         } catch (SQLException var3) {
            logger.info("Call to cancel query on RDBMs source failed with error: {}", var3.getMessage());
            logger.debug("Call to cancel query on RDBMs source failed with error: {}", var3.getMessage(), var3);
         }

      }
   }

   public void close() throws Exception {
      try {
         this.cancel();
      } finally {
         AutoCloseables.close(new AutoCloseable[]{this.resultSet, this.statement, this.connection});
      }

   }

   public void checkSchemaConsistency(List<JdbcToFieldMapping> mappings) {
      List mappingsFieldNames;
      if (mappings.size() > this.columns.size()) {
         logger.debug("More columns returned from generated SQL than from query planning. Checking if these are all skipped columns.");
         mappingsFieldNames = this.identifyUnexpectedColumns(mappings);
         if (!mappingsFieldNames.isEmpty()) {
            logger.debug("Unrecognized columns returned from generated SQL than from query planning. Throwing invalid metadata error to trigger refresh of columns.");
            throw UserException.invalidMetadataError().addContext("Unexpected columns", Strings.join(mappingsFieldNames, ",")).addContext("Expected skipped columns", Strings.join(this.skippedColumns, ",")).setAdditionalExceptionContext(new InvalidMetadataErrorContext(ImmutableList.copyOf(this.tableList))).build(logger);
         }
      } else if (mappings.size() < this.columns.size()) {
         logger.debug("Fewer columns returned from generated SQL than from query planning. Throwing invalid metadata error to trigger refresh of columns.");
         mappingsFieldNames = this.identifyUnexpectedColumns(mappings);
         com.dremio.common.exceptions.UserException.Builder builder = UserException.invalidMetadataError();
         if (!mappingsFieldNames.isEmpty()) {
            builder.addContext("Unexpected columns: %s", Strings.join(mappingsFieldNames, ","));
         }

         throw builder.addContext("Expected skipped columns: ", Strings.join(this.skippedColumns, ",")).setAdditionalExceptionContext(new InvalidMetadataErrorContext(ImmutableList.copyOf(this.tableList))).build(logger);
      }

      mappingsFieldNames = (List)mappings.stream().map((m) -> {
         return m.getField().getName().toLowerCase(Locale.ROOT);
      }).collect(Collectors.toList());
      Preconditions.checkArgument(this.columns.size() == mappingsFieldNames.size());

      for(int i = 0; i < this.columns.size(); ++i) {
         Preconditions.checkArgument(((SchemaPath)this.columns.get(i)).getAsUnescapedPath().toLowerCase(Locale.ROOT).equals(mappingsFieldNames.get(i)));
      }

   }

   private void addFieldToOutput(OutputMutator output, Builder<ValueVector> valueVectorBuilder, Builder<JdbcRecordReader.Copier<?>> copierBuilder, JdbcToFieldMapping mapping) {
      Field field = mapping.getField();
      Class<? extends ValueVector> clazz = TypeHelper.getValueVectorClass(field);
      ValueVector vector = output.addField(field, clazz);
      valueVectorBuilder.add(vector);
      copierBuilder.add(this.getCopier(mapping.getJdbcOrdinal(), this.resultSet, vector));
   }

   private List<String> identifyUnexpectedColumns(List<JdbcToFieldMapping> jdbcMappings) {
      List<String> expectedColumns = (List)this.columns.stream().map(BasePath::getAsUnescapedPath).map(String::toLowerCase).collect(Collectors.toList());
      Builder<String> unexpectedColumnListBuilder = ImmutableList.builder();
      Iterator var4 = jdbcMappings.iterator();

      while(var4.hasNext()) {
         JdbcToFieldMapping mapping = (JdbcToFieldMapping)var4.next();
         String columnName = mapping.getField().getName().toLowerCase(Locale.ROOT);
         if (!expectedColumns.contains(columnName) && !this.skippedColumns.contains(columnName)) {
            logger.debug("Unrecognized column name found: {}.", columnName);
            unexpectedColumnListBuilder.add(columnName);
         }
      }

      return unexpectedColumnListBuilder.build();
   }

   @VisibleForTesting
   public void throwInvalidMetadataError(String message) {
      throw UserException.invalidMetadataError().addContext(message).setAdditionalExceptionContext(new InvalidMetadataErrorContext(ImmutableList.copyOf(this.tableList))).buildSilently();
   }

   @VisibleForTesting
   static BigDecimal processDecimal(BigDecimal originalValue, int vectorScale, int originalValueScale) {
      BigDecimal scaledDecimalValue = getScaledDecimalValue(originalValue, vectorScale, originalValueScale);
      int finalValuePrecision = scaledDecimalValue.precision();
      if (finalValuePrecision > 38) {
         throw new IllegalArgumentException(String.format("Received a Decimal value with precision %d that exceeds the maximum supported precision of %d. Please try adding an explicit cast or reducing the volume of data processed in the query.", finalValuePrecision, 38));
      } else {
         return scaledDecimalValue;
      }
   }

   private static BigDecimal getScaledDecimalValue(BigDecimal originalValue, int vectorScale, int originalValueScale) {
      if (originalValueScale < vectorScale) {
         return originalValue.setScale(vectorScale);
      } else {
         return originalValueScale > vectorScale ? originalValue.setScale(vectorScale, 5) : originalValue;
      }
   }

   private static int getTrimmedSize(byte[] record, byte b) {
      for(int i = record.length; i > 0; --i) {
         if (record[i - 1] != b) {
            return i;
         }
      }

      return 0;
   }

   @VisibleForTesting
   static ZonedDateTime treatAsUTC(Timestamp timestamp) {
      ZoneId zoneSystemDefault = ZoneId.systemDefault();
      LocalDateTime localDateTime = timestamp.toLocalDateTime();
      ZonedDateTime zonedUTC = ZonedDateTime.ofLocal(localDateTime, ZoneId.of("UTC"), (ZoneOffset)null);
      return zonedUTC.withZoneSameInstant(zoneSystemDefault);
   }

   private static class BitCopier extends JdbcRecordReader.Copier<BitVector> {
      BitCopier(int columnIndex, ResultSet result, BitVector vector) {
         super(columnIndex, result, vector);
      }

      void copy(int index) throws SQLException {
         ((BitVector)this.getValueVector()).setSafe(index, this.getResult().getBoolean(this.getColumnIndex()) ? 1 : 0);
         if (this.getResult().wasNull()) {
            ((BitVector)this.getValueVector()).setNull(index);
         }

      }
   }

   private static class TimeStampCopierCoerceToUTC extends JdbcRecordReader.Copier<TimeStampMilliVector> {
      private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

      TimeStampCopierCoerceToUTC(int columnIndex, ResultSet result, TimeStampMilliVector vector) {
         super(columnIndex, result, vector);
      }

      void copy(int index) throws SQLException {
         Timestamp stamp = this.getResult().getTimestamp(this.getColumnIndex(), this.calendar);
         if (stamp != null) {
            Timestamp newTimestamp = Timestamp.valueOf(JdbcRecordReader.treatAsUTC(stamp).toLocalDateTime());
            ((TimeStampMilliVector)this.getValueVector()).setSafe(index, newTimestamp.getTime());
         }

      }
   }

   private static class TimeStampCopier extends JdbcRecordReader.Copier<TimeStampMilliVector> {
      private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

      TimeStampCopier(int columnIndex, ResultSet result, TimeStampMilliVector vector) {
         super(columnIndex, result, vector);
      }

      void copy(int index) throws SQLException {
         Timestamp stamp = this.getResult().getTimestamp(this.getColumnIndex(), this.calendar);
         if (stamp != null) {
            ((TimeStampMilliVector)this.getValueVector()).setSafe(index, stamp.getTime());
         }

      }
   }

   private static class TimeCopierCoerceToUTC extends JdbcRecordReader.Copier<TimeMilliVector> {
      private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

      TimeCopierCoerceToUTC(int columnIndex, ResultSet result, TimeMilliVector vector) {
         super(columnIndex, result, vector);
      }

      void copy(int index) throws SQLException {
         Timestamp stamp = this.getResult().getTimestamp(this.getColumnIndex(), this.calendar);
         if (stamp != null) {
            LocalTime localTime = JdbcRecordReader.treatAsUTC(stamp).toLocalTime();
            ((TimeMilliVector)this.getValueVector()).setSafe(index, (int)(Time.valueOf(localTime).getTime() + (long)(localTime.getNano() / 1000000)));
         }

      }
   }

   private static class TimeCopier extends JdbcRecordReader.Copier<TimeMilliVector> {
      private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

      TimeCopier(int columnIndex, ResultSet result, TimeMilliVector vector) {
         super(columnIndex, result, vector);
      }

      void copy(int index) throws SQLException {
         Time time = this.getResult().getTime(this.getColumnIndex(), this.calendar);
         if (time != null) {
            ((TimeMilliVector)this.getValueVector()).setSafe(index, (int)time.getTime());
         }

      }
   }

   private static class DateTimeZoneAdjustmentCopier extends JdbcRecordReader.Copier<DateMilliVector> {
      private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

      DateTimeZoneAdjustmentCopier(int columnIndex, ResultSet result, DateMilliVector vector) {
         super(columnIndex, result, vector);
      }

      void copy(int index) throws SQLException {
         Date date = this.getResult().getDate(this.getColumnIndex(), this.calendar);
         if (date != null) {
            ((DateMilliVector)this.getValueVector()).setSafe(index, date.getTime() + (long)TimeZone.getDefault().getOffset(date.getTime()));
         }

      }
   }

   private static class DateCopier extends JdbcRecordReader.Copier<DateMilliVector> {
      private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

      DateCopier(int columnIndex, ResultSet result, DateMilliVector vector) {
         super(columnIndex, result, vector);
      }

      void copy(int index) throws SQLException {
         Date date = this.getResult().getDate(this.getColumnIndex(), this.calendar);
         if (date != null) {
            ((DateMilliVector)this.getValueVector()).setSafe(index, date.getTime());
         }

      }
   }

   private static class VarBinaryCopier extends JdbcRecordReader.Copier<VarBinaryVector> {
      private final int maxCellSize;

      VarBinaryCopier(int maxCellSize, int columnIndex, ResultSet result, VarBinaryVector vector) {
         super(columnIndex, result, vector);
         this.maxCellSize = maxCellSize;
      }

      void copy(int index) throws SQLException {
         byte[] record = this.getResult().getBytes(this.getColumnIndex());
         if (record != null) {
            FieldSizeLimitExceptionHelper.checkSizeLimit(record.length, this.maxCellSize, this.getColumnIndex(), JdbcRecordReader.logger);
            ((VarBinaryVector)this.getValueVector()).setSafe(index, record, 0, record.length);
         }

      }
   }

   private static class VarCharTrimCopier extends JdbcRecordReader.Copier<VarCharVector> {
      private final int maxCellSize;

      VarCharTrimCopier(int maxCellSize, int columnIndex, ResultSet result, VarCharVector vector) {
         super(columnIndex, result, vector);
         this.maxCellSize = maxCellSize;
      }

      void copy(int index) throws SQLException {
         String val = this.getResult().getString(this.getColumnIndex());
         if (val != null) {
            byte[] record = val.getBytes(Charsets.UTF_8);
            int trimmedSize = JdbcRecordReader.getTrimmedSize(record, (byte)32);
            FieldSizeLimitExceptionHelper.checkSizeLimit(trimmedSize, this.maxCellSize, this.getColumnIndex(), JdbcRecordReader.logger);
            ((VarCharVector)this.getValueVector()).setSafe(index, record, 0, trimmedSize);
         }

      }
   }

   private static class VarCharCopier extends JdbcRecordReader.Copier<VarCharVector> {
      private final int maxCellSize;

      VarCharCopier(int maxCellSize, int columnIndex, ResultSet result, VarCharVector vector) {
         super(columnIndex, result, vector);
         this.maxCellSize = maxCellSize;
      }

      void copy(int index) throws SQLException {
         String val = this.getResult().getString(this.getColumnIndex());
         if (val != null) {
            byte[] record = val.getBytes(Charsets.UTF_8);
            FieldSizeLimitExceptionHelper.checkSizeLimit(record.length, this.maxCellSize, this.getColumnIndex(), JdbcRecordReader.logger);
            ((VarCharVector)this.getValueVector()).setSafe(index, record, 0, record.length);
         }

      }
   }

   private static class DecimalCopier extends JdbcRecordReader.Copier<DecimalVector> {
      DecimalCopier(int columnIndex, ResultSet result, DecimalVector vector) {
         super(columnIndex, result, vector);
      }

      void copy(int index) throws SQLException {
         BigDecimal originalValue = this.getResult().getBigDecimal(this.getColumnIndex());
         if (originalValue != null) {
            int vectorScale = ((DecimalVector)this.getValueVector()).getScale();
            int originalValueScale = originalValue.scale();

            try {
               ((DecimalVector)this.getValueVector()).setSafe(index, JdbcRecordReader.processDecimal(originalValue, vectorScale, originalValueScale));
            } catch (UnsupportedOperationException var6) {
               throw new IllegalArgumentException(String.format("Expected a Decimal value with precision %d and scale %d but received a value with precision %d and scale %d. Please try adding an explicit cast to your query.", ((DecimalVector)this.getValueVector()).getPrecision(), vectorScale, originalValue.precision(), originalValueScale), var6);
            }
         }

      }
   }

   private static class Float8Copier extends JdbcRecordReader.Copier<Float8Vector> {
      Float8Copier(int columnIndex, ResultSet result, Float8Vector vector) {
         super(columnIndex, result, vector);
      }

      void copy(int index) throws SQLException {
         ((Float8Vector)this.getValueVector()).setSafe(index, this.getResult().getDouble(this.getColumnIndex()));
         if (this.getResult().wasNull()) {
            ((Float8Vector)this.getValueVector()).setNull(index);
         }

      }
   }

   private static class Float4Copier extends JdbcRecordReader.Copier<Float4Vector> {
      Float4Copier(int columnIndex, ResultSet result, Float4Vector vector) {
         super(columnIndex, result, vector);
      }

      void copy(int index) throws SQLException {
         ((Float4Vector)this.getValueVector()).setSafe(index, this.getResult().getFloat(this.getColumnIndex()));
         if (this.getResult().wasNull()) {
            ((Float4Vector)this.getValueVector()).setNull(index);
         }

      }
   }

   private static class BigIntCopier extends JdbcRecordReader.Copier<BigIntVector> {
      BigIntCopier(int offset, ResultSet set, BigIntVector vector) {
         super(offset, set, vector);
      }

      void copy(int index) throws SQLException {
         ((BigIntVector)this.getValueVector()).setSafe(index, this.getResult().getLong(this.getColumnIndex()));
         if (this.getResult().wasNull()) {
            ((BigIntVector)this.getValueVector()).setNull(index);
         }

      }
   }

   private static class IntCopier extends JdbcRecordReader.Copier<IntVector> {
      IntCopier(int offset, ResultSet set, IntVector vector) {
         super(offset, set, vector);
      }

      void copy(int index) throws SQLException {
         ((IntVector)this.getValueVector()).setSafe(index, this.getResult().getInt(this.getColumnIndex()));
         if (this.getResult().wasNull()) {
            ((IntVector)this.getValueVector()).setNull(index);
         }

      }
   }

   private abstract static class Copier<T extends ValueVector> {
      private final int columnIndex;
      private final ResultSet result;
      private final T valueVector;

      Copier(int columnIndex, ResultSet result, T valueVector) {
         this.columnIndex = columnIndex;
         this.result = result;
         this.valueVector = valueVector;
      }

      abstract void copy(int var1) throws SQLException;

      protected int getColumnIndex() {
         return this.columnIndex;
      }

      protected ResultSet getResult() {
         return this.result;
      }

      protected T getValueVector() {
         return this.valueVector;
      }
   }
}
