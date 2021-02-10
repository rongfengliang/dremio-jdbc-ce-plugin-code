package com.dremio.exec.store.jdbc;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.store.StoragePluginUtils;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.ColumnProperties;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.GetTableMetadataRequest;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.GetTableMetadataResponse;
import com.dremio.exec.store.jdbc.dialect.JdbcToFieldMapping;
import com.dremio.exec.store.jdbc.dialect.SourceTypeDescriptor;
import com.dremio.exec.store.jdbc.dialect.TableSourceTypeDescriptor;
import com.dremio.exec.store.jdbc.dialect.TypeMapper;
import com.dremio.exec.store.jdbc.legacy.JdbcDremioSqlDialect;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.ImmutableList.Builder;
import com.google.protobuf.ByteString;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDatasetMetadata {
   private static final Logger logger = LoggerFactory.getLogger(JdbcDatasetMetadata.class);
   private final JdbcSchemaFetcherImpl fetcher;
   private final GetTableMetadataRequest request;
   private final List<String> identifiers;
   private List<String> skippedColumns;
   private List<ColumnProperties> columnProperties;
   private List<JdbcToFieldMapping> jdbcToFieldMappings;

   JdbcDatasetMetadata(JdbcSchemaFetcherImpl fetcher, GetTableMetadataRequest request) {
      this.fetcher = fetcher;
      this.request = request;
      List<String> identifiers = Lists.newArrayList();
      if (!Strings.isNullOrEmpty(request.getCatalog())) {
         identifiers.add(request.getCatalog());
      }

      if (!Strings.isNullOrEmpty(request.getSchema())) {
         identifiers.add(request.getSchema());
      }

      identifiers.add(request.getTable());
      this.identifiers = identifiers;
   }

   public GetTableMetadataResponse build() {
      Stopwatch watch = Stopwatch.createStarted();
      if (this.fetcher.usePrepareForColumnMetadata()) {
         this.prepareColumnMetadata();
      } else {
         this.apiColumnMetadata();
      }

      logger.info("Took {} ms to get column metadata for {}", watch.elapsed(TimeUnit.MILLISECONDS), this.identifiers);
      watch.reset().start();
      long recordCount = this.fetcher.getRowCount(this.identifiers);
      logger.info("Took {} ms to get row count for {}", watch.elapsed(TimeUnit.MILLISECONDS), this.identifiers);
      Schema recordSchema = new Schema((Iterable)this.jdbcToFieldMappings.stream().map(JdbcToFieldMapping::getField).collect(Collectors.toList()));
      return GetTableMetadataResponse.newBuilder().addAllSkippedColumns(this.skippedColumns).addAllColumnProperties(this.columnProperties).setRecordCount(recordCount).setRecordSchema(ByteString.copyFrom(recordSchema.toByteArray())).build();
   }

   private JdbcDremioSqlDialect getDialect() {
      return (JdbcDremioSqlDialect)this.fetcher.config.getDialect();
   }

   private void apiColumnMetadata() {
      this.getColumnMetadata((connection, skippedColumnBuilder, propertiesBuilders) -> {
         return this.getDialect().getDataTypeMapper().mapJdbcToArrowFields((sourceTypeDescriptor, shouldSkip) -> {
            this.handleUnpushableColumn(skippedColumnBuilder, propertiesBuilders, sourceTypeDescriptor, shouldSkip);
         }, (colName, processor) -> {
            this.processColumnProperty(propertiesBuilders, colName, processor);
         }, connection, this.request.getCatalog(), this.request.getSchema(), this.request.getTable(), false);
      });
   }

   private void prepareColumnMetadata() {
      this.getColumnMetadata((connection, skippedColumnBuilder, propertiesBuilders) -> {
         String quotedPath = this.fetcher.getQuotedPath(this.identifiers);
         PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + quotedPath);
         Throwable var6 = null;

         List var7;
         try {
            var7 = this.getDialect().getDataTypeMapper().mapJdbcToArrowFields((sourceTypeDescriptor, shouldSkip) -> {
               this.handleUnpushableColumn(skippedColumnBuilder, propertiesBuilders, sourceTypeDescriptor, shouldSkip);
            }, (colName, processor) -> {
               this.processColumnProperty(propertiesBuilders, colName, processor);
            }, (message) -> {
               throw new IllegalArgumentException(message);
            }, connection, this.request.getCatalog(), this.request.getSchema(), this.request.getTable(), statement.getMetaData(), (Set)null, false, false);
         } catch (Throwable var11) {
            var6 = var11;
            throw var11;
         } finally {
            if (statement != null) {
               $closeResource(var6, statement);
            }

         }

         return var7;
      });
   }

   private void getColumnMetadata(JdbcDatasetMetadata.MapFunction mapFields) {
      try {
         Connection connection = this.fetcher.dataSource.getConnection();
         Throwable var3 = null;

         try {
            Builder<String> skippedColumnBuilder = ImmutableList.builder();
            Map<String, com.dremio.exec.store.jdbc.JdbcFetcherProto.ColumnProperties.Builder> propertiesBuilders = Maps.newHashMap();
            this.jdbcToFieldMappings = mapFields.map(connection, skippedColumnBuilder, propertiesBuilders);
            this.columnProperties = (List)propertiesBuilders.values().stream().map(com.dremio.exec.store.jdbc.JdbcFetcherProto.ColumnProperties.Builder::build).collect(Collectors.toList());
            this.skippedColumns = skippedColumnBuilder.build();
         } catch (Throwable var10) {
            var3 = var10;
            throw var10;
         } finally {
            if (connection != null) {
               $closeResource(var3, connection);
            }

         }

      } catch (SQLException var12) {
         throw StoragePluginUtils.message(UserException.dataReadError(var12), this.fetcher.config.getSourceName(), "Failed getting columns for %s.", new Object[]{this.fetcher.getQuotedPath(this.identifiers)}).build(logger);
      }
   }

   private void handleUnpushableColumn(Builder<String> skippedColumnBuilder, Map<String, com.dremio.exec.store.jdbc.JdbcFetcherProto.ColumnProperties.Builder> propertiesBuilders, SourceTypeDescriptor sourceTypeDescriptor, boolean shouldSkip) {
      if (shouldSkip) {
         skippedColumnBuilder.add(sourceTypeDescriptor.getFieldName().toLowerCase(Locale.ROOT));
         this.warnUnsupportedColumnType(sourceTypeDescriptor);
      } else {
         this.processColumnProperty(propertiesBuilders, sourceTypeDescriptor.getFieldName(), ColumnPropertiesProcessors.UNPUSHABLE_COLUMN);
      }

   }

   private void processColumnProperty(Map<String, com.dremio.exec.store.jdbc.JdbcFetcherProto.ColumnProperties.Builder> builderMap, String columnName, ColumnPropertiesProcessor processor) {
      String canonicalColumnName = columnName.toLowerCase(Locale.ROOT);
      com.dremio.exec.store.jdbc.JdbcFetcherProto.ColumnProperties.Builder builder = (com.dremio.exec.store.jdbc.JdbcFetcherProto.ColumnProperties.Builder)builderMap.computeIfAbsent(canonicalColumnName, (i) -> {
         return ColumnProperties.newBuilder().setColumnName(canonicalColumnName);
      });
      processor.process(builder);
   }

   protected void warnUnsupportedColumnType(SourceTypeDescriptor type) {
      TableSourceTypeDescriptor tableDescriptor = (TableSourceTypeDescriptor)type.unwrap(TableSourceTypeDescriptor.class);
      String columnName;
      if (tableDescriptor != null) {
         columnName = String.format("%s.%s.%s.%s", tableDescriptor.getCatalog(), tableDescriptor.getSchema(), tableDescriptor.getTable(), type.getFieldName());
      } else {
         columnName = type.getFieldName();
      }

      logger.warn("A column you queried has a data type that is not currently supported by the JDBC storage plugin. The column's name was {}, its JDBC data type was {}, and the source column type was {}.", new Object[]{columnName, TypeMapper.nameFromType(type.getReportedJdbcType()), type.getDataSourceTypeName()});
   }

   // $FF: synthetic method
   private static void $closeResource(Throwable x0, AutoCloseable x1) {
      if (x0 != null) {
         try {
            x1.close();
         } catch (Throwable var3) {
            x0.addSuppressed(var3);
         }
      } else {
         x1.close();
      }

   }

   @FunctionalInterface
   private interface MapFunction {
      List<JdbcToFieldMapping> map(Connection var1, Builder<String> var2, Map<String, com.dremio.exec.store.jdbc.JdbcFetcherProto.ColumnProperties.Builder> var3) throws SQLException;
   }
}
