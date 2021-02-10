package com.dremio.exec.store.jdbc.dialect;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.store.jdbc.ColumnPropertiesProcessor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TypeMapper {
   public static final String TO_DATE_NAME = "TO_DATE";
   public static final String DATE_TRUNC_NAME = "DATE_TRUNC";
   public static final String REGEXP_PREFIX = "REGEXP_";
   private static final Logger logger = LoggerFactory.getLogger(TypeMapper.class);
   protected final boolean useDecimalToDoubleMapping;

   public TypeMapper(boolean useDecimalToDoubleMapping) {
      this.useDecimalToDoubleMapping = useDecimalToDoubleMapping;
   }

   public final List<JdbcToFieldMapping> mapJdbcToArrowFields(TypeMapper.UnrecognizedTypeMarker unrecognizedTypeCallback, TypeMapper.AddPropertyCallback addColumnPropertyCallback, Connection connection, String catalog, String schema, String table, boolean mapSkippedColumnsAsNullType) throws SQLException {
      List<SourceTypeDescriptor> sourceTypes = this.convertGetColumnsCallToSourceTypeDescriptors(addColumnPropertyCallback, connection, new TypeMapper.TableIdentifier(catalog, schema, table));
      return this.mapSourceToArrowFields(this.handleUnknownType(unrecognizedTypeCallback), addColumnPropertyCallback, sourceTypes, mapSkippedColumnsAsNullType);
   }

   public final List<JdbcToFieldMapping> mapJdbcToArrowFields(TypeMapper.UnrecognizedTypeMarker unrecognizedTypeCallback, TypeMapper.AddPropertyCallback addColumnPropertyCallback, TypeMapper.InvalidMetaDataCallback invalidMetaDataCallback, Connection connection, ResultSetMetaData metaData, Set<String> unsupportedColumns, boolean mapSkippedColumnsAsNullType) throws SQLException {
      return this.mapJdbcToArrowFields(unrecognizedTypeCallback, addColumnPropertyCallback, invalidMetaDataCallback, connection, (String)null, (String)null, (String)null, metaData, unsupportedColumns, true, mapSkippedColumnsAsNullType);
   }

   public final List<JdbcToFieldMapping> mapJdbcToArrowFields(TypeMapper.UnrecognizedTypeMarker unrecognizedTypeCallback, TypeMapper.AddPropertyCallback addColumnPropertyCallback, TypeMapper.InvalidMetaDataCallback invalidMetaDataCallback, Connection connection, String catalog, String schema, String table, ResultSetMetaData metaData, Set<String> unsupportedColumns, boolean changeColumnCasing, boolean mapSkippedColumnsAsNullType) throws SQLException {
      List<SourceTypeDescriptor> sourceTypes = this.convertResultSetMetaDataToSourceTypeDescriptors(addColumnPropertyCallback, invalidMetaDataCallback, connection, catalog, schema, table, metaData, unsupportedColumns, changeColumnCasing);
      return this.mapSourceToArrowFields(this.handleUnknownType(unrecognizedTypeCallback), addColumnPropertyCallback, sourceTypes, mapSkippedColumnsAsNullType);
   }

   protected abstract List<JdbcToFieldMapping> mapSourceToArrowFields(TypeMapper.UnrecognizedTypeCallback var1, TypeMapper.AddPropertyCallback var2, List<SourceTypeDescriptor> var3, boolean var4);

   private List<SourceTypeDescriptor> convertResultSetMetaDataToSourceTypeDescriptors(TypeMapper.AddPropertyCallback addColumnPropertyCallback, TypeMapper.InvalidMetaDataCallback invalidMetaDataCallback, Connection connection, String catalog, String schema, String table, ResultSetMetaData metaData, Set<String> unsupportedColumns, boolean changeColumnCasing) throws SQLException {
      logger.debug("Getting column types from metadata for table with catalogName=[{}], schemaName=[{}], tableName=[{}].", new Object[]{catalog, schema, table});
      Builder<SourceTypeDescriptor> builder = ImmutableList.builder();
      Set<String> usedNames = new HashSet();
      Set<String> unsupportedColumns = unsupportedColumns == null ? new HashSet() : (!changeColumnCasing ? unsupportedColumns : (Set)unsupportedColumns.stream().map((skippedColumn) -> {
         return skippedColumn.toLowerCase(Locale.ROOT);
      }).collect(Collectors.toSet()));

      for(int i = 1; i <= metaData.getColumnCount(); ++i) {
         String columnLabel = metaData.getColumnLabel(i);
         if (changeColumnCasing) {
            columnLabel = columnLabel.toLowerCase(Locale.ROOT);
         }

         if (!((Set)unsupportedColumns).contains(columnLabel)) {
            columnLabel = SqlValidatorUtil.uniquify(columnLabel, usedNames, SqlValidatorUtil.EXPR_SUGGESTER);
            usedNames.add(columnLabel);
            builder.add(this.createTypeDescriptor(addColumnPropertyCallback, invalidMetaDataCallback, connection, new TypeMapper.TableIdentifier(catalog, schema, table), metaData, columnLabel, i));
         }
      }

      return builder.build();
   }

   protected SourceTypeDescriptor createTypeDescriptor(TypeMapper.AddPropertyCallback addColumnPropertyCallback, TypeMapper.InvalidMetaDataCallback invalidMetaDataCallback, Connection connection, TypeMapper.TableIdentifier table, ResultSetMetaData metaData, String columnLabel, int colIndex) throws SQLException {
      return new SourceTypeDescriptor(columnLabel, metaData.getColumnType(colIndex), metaData.getColumnTypeName(colIndex), colIndex, metaData.getPrecision(colIndex), metaData.getScale(colIndex));
   }

   protected SourceTypeDescriptor createTableTypeDescriptor(TypeMapper.AddPropertyCallback addColumnPropertyCallback, Connection connection, String columnName, int sourceJdbcType, String typeString, TypeMapper.TableIdentifier table, int colIndex, int precision, int scale) {
      return new TableSourceTypeDescriptor(columnName, sourceJdbcType, typeString, table.catalog, table.schema, table.table, colIndex, precision, scale);
   }

   private List<SourceTypeDescriptor> convertGetColumnsCallToSourceTypeDescriptors(TypeMapper.AddPropertyCallback addColumnPropertyCallback, Connection connection, TypeMapper.TableIdentifier table) throws SQLException {
      logger.debug("Getting column types from getColumns for tables with catalogName=[{}], schemaName=[{}], tableName=[{}].", new Object[]{table.catalog, table.schema, table.table});
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet getColumnsResultSet = metaData.getColumns(table.catalog, table.schema, table.table, "%");
      Throwable var6 = null;

      ImmutableList var26;
      try {
         Builder<SourceTypeDescriptor> builder = ImmutableList.builder();
         int jdbcOrdinal = 0;
         String newCatalog = "";
         String newSchema = "";

         String columnName;
         int sourceJdbcType;
         String typeString;
         int precision;
         int scale;
         for(String newTable = ""; getColumnsResultSet.next(); builder.add(this.createTableTypeDescriptor(addColumnPropertyCallback, connection, columnName, sourceJdbcType, typeString, new TypeMapper.TableIdentifier(newCatalog, newSchema, newTable), jdbcOrdinal, precision, scale))) {
            if (jdbcOrdinal == 0) {
               newCatalog = getColumnsResultSet.getString(1);
               newSchema = getColumnsResultSet.getString(2);
               newTable = getColumnsResultSet.getString(3);
            }

            ++jdbcOrdinal;
            columnName = getColumnsResultSet.getString(4);
            sourceJdbcType = getColumnsResultSet.getInt(5);
            typeString = getColumnsResultSet.getString(6);
            if (sourceJdbcType != 3 && sourceJdbcType != 2) {
               precision = 0;
               scale = 0;
            } else {
               precision = getColumnsResultSet.getInt(7);
               scale = getColumnsResultSet.getInt(9);
            }
         }

         var26 = builder.build();
      } catch (Throwable var24) {
         var6 = var24;
         throw var24;
      } finally {
         if (getColumnsResultSet != null) {
            if (var6 != null) {
               try {
                  getColumnsResultSet.close();
               } catch (Throwable var23) {
                  var6.addSuppressed(var23);
               }
            } else {
               getColumnsResultSet.close();
            }
         }

      }

      return var26;
   }

   public static String nameFromType(int javaSqlType) {
      try {
         Field[] var1 = Types.class.getFields();
         int var2 = var1.length;

         for(int var3 = 0; var3 < var2; ++var3) {
            Field f = var1[var3];
            if (Modifier.isStatic(f.getModifiers()) && f.getType() == Integer.TYPE && f.getInt((Object)null) == javaSqlType) {
               return f.getName();
            }
         }
      } catch (IllegalAccessException | IllegalArgumentException var5) {
      }

      return Integer.toString(javaSqlType);
   }

   private TypeMapper.UnrecognizedTypeCallback handleUnknownType(TypeMapper.UnrecognizedTypeMarker unrecognizedTypeCallback) {
      return (sourceTypeDescriptor, shouldSkip) -> {
         if (unrecognizedTypeCallback != null) {
            unrecognizedTypeCallback.markUnrecognized(sourceTypeDescriptor, shouldSkip);
         }

         switch(sourceTypeDescriptor.getReportedJdbcType()) {
         case -16:
         case -15:
         case -9:
         case -1:
         case 1:
         case 12:
         default:
            return CompleteType.VARCHAR;
         case -8:
         case -5:
            return CompleteType.BIGINT;
         case -7:
         case 16:
            return CompleteType.BIT;
         case -6:
         case 4:
         case 5:
            return CompleteType.INT;
         case -4:
         case -3:
         case -2:
         case 2004:
            return CompleteType.VARBINARY;
         case 2:
         case 3:
            int prec = sourceTypeDescriptor.getPrecision();
            int scale = sourceTypeDescriptor.getScale();
            if (prec > 38) {
               if (scale > 6) {
                  scale = Math.max(6, scale - (prec - 38));
               }

               prec = 38;
            }

            return CompleteType.fromDecimalPrecisionScale(prec, scale);
         case 6:
         case 7:
            return CompleteType.FLOAT;
         case 8:
            return CompleteType.DOUBLE;
         case 91:
            return CompleteType.DATE;
         case 92:
            return CompleteType.TIME;
         case 93:
            return CompleteType.TIMESTAMP;
         }
      };
   }

   public class TableIdentifier {
      public final String catalog;
      public final String schema;
      public final String table;

      public TableIdentifier(String catalog, String schema, String table) {
         this.catalog = catalog;
         this.schema = schema;
         this.table = table;
      }
   }

   @FunctionalInterface
   public interface InvalidMetaDataCallback {
      void throwOnInvalidMetaData(String var1);
   }

   @FunctionalInterface
   public interface UnrecognizedTypeCallback {
      CompleteType mark(SourceTypeDescriptor var1, boolean var2);
   }

   @FunctionalInterface
   public interface UnrecognizedTypeMarker {
      void markUnrecognized(SourceTypeDescriptor var1, boolean var2);
   }

   @FunctionalInterface
   public interface AddPropertyCallback {
      void addProperty(String var1, ColumnPropertiesProcessor var2);
   }
}
