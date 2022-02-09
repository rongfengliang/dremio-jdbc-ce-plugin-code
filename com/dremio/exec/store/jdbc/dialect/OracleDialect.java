package com.dremio.exec.store.jdbc.dialect;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.store.jdbc.ColumnPropertiesProcessors;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.CanonicalizeTablePathRequest;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.CanonicalizeTablePathResponse;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpTypeMapper;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import com.dremio.exec.store.jdbc.rel2sql.OracleRelToSqlConverter;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OracleDialect extends ArpDialect {
   private static final int ORACLE_MAX_VARCHAR_LENGTH = 4000;
   private static final Integer ORACLE_MAX_IDENTIFIER_LENGTH = 30;
   public static final String MAP_DATE_TO_TIMESTAMP = "MAP_DATE_TO_TIMESTAMP";
   private static final String TIMESTAMP = "TIMESTAMP";

   public OracleDialect(ArpYaml yaml) {
      super(yaml);
   }

   public SqlNode emulateNullDirection(SqlNode node, boolean nullsFirst, boolean desc) {
      return null;
   }

   public SqlNode getCastSpec(RelDataType type) {
      switch(type.getSqlTypeName()) {
      case VARCHAR:
         if (type.getPrecision() <= 4000 && type.getPrecision() != -1) {
            return getVarcharWithPrecision(this, type, type.getPrecision());
         }

         return getVarcharWithPrecision(this, type, 4000);
      case DOUBLE:
         return new SqlDataTypeSpec(new SqlIdentifier(SqlTypeName.FLOAT.name(), SqlParserPos.ZERO), -1, -1, (String)null, (TimeZone)null, SqlParserPos.ZERO);
      case INTEGER:
      case BIGINT:
         return new SqlDataTypeSpec(new SqlIdentifier(SqlTypeName.DECIMAL.name(), SqlParserPos.ZERO), 38, 0, (String)null, (TimeZone)null, SqlParserPos.ZERO);
      case TIME:
      case DATE:
         return new SqlDataTypeSpec(new SqlIdentifier("TIMESTAMP", SqlParserPos.ZERO), -1, -1, (String)null, (TimeZone)null, SqlParserPos.ZERO) {
            public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
               writer.keyword("TIMESTAMP");
            }
         };
      default:
         return super.getCastSpec(type);
      }
   }

   public TypeMapper getDataTypeMapper(JdbcPluginConfig config) {
      return new OracleDialect.OracleTypeMapper(this.getYaml(), Boolean.parseBoolean((String)config.getGenericProperties().getOrDefault("MAP_DATE_TO_TIMESTAMP", Boolean.FALSE.toString())));
   }

   public boolean supportsAliasedValues() {
      return false;
   }

   protected boolean allowsAs() {
      return false;
   }

   public OracleRelToSqlConverter getConverter() {
      return new OracleRelToSqlConverter(this);
   }

   public ArpDialect.ArpSchemaFetcher newSchemaFetcher(JdbcPluginConfig config) {
      StringBuilder queryBuilder = new StringBuilder("SELECT * FROM (SELECT NULL CAT, OWNER SCH, TABLE_NAME NME FROM SYS.ALL_ALL_TABLES UNION ALL SELECT NULL CAT, OWNER SCH, VIEW_NAME NME FROM SYS.ALL_VIEWS");
      if (!config.getHiddenTableTypes().contains("SYNONYM")) {
         queryBuilder.append(" UNION ALL SELECT NULL CAT, OWNER SCH, SYNONYM_NAME NME FROM SYS.ALL_SYNONYMS");
      }

      queryBuilder.append(") WHERE SCH NOT IN ('%s')");
      String query = String.format(queryBuilder.toString(), Joiner.on("','").join(config.getHiddenSchemas()));
      return new OracleDialect.OracleSchemaFetcher(query, config);
   }

   public boolean supportsLiteral(CompleteType type) {
      if (!CompleteType.INT.equals(type) && !CompleteType.BIGINT.equals(type)) {
         return CompleteType.DATE.equals(type) ? true : super.supportsLiteral(type);
      } else {
         return true;
      }
   }

   public boolean supportsBooleanAggregation() {
      return false;
   }

   public boolean supportsRegexString(String regex) {
      int index = 0;

      while(-1 != (index = regex.indexOf(92, index))) {
         if (index >= regex.length() - 1) {
            return true;
         }

         char escaped = regex.charAt(index + 1);
         if (Character.isLetter(escaped)) {
            switch(escaped) {
            case 'E':
            case 'Q':
               return false;
            default:
               ++index;
            }
         }
      }

      return true;
   }

   public boolean supportsSort(boolean isCollationEmpty, boolean isOffsetEmpty) {
      return isOffsetEmpty;
   }

   public Integer getIdentifierLengthLimit() {
      return ORACLE_MAX_IDENTIFIER_LENGTH;
   }

   private static class OracleTypeMapper extends ArpTypeMapper {
      private final boolean mapDateToTimestamp;

      public OracleTypeMapper(ArpYaml yaml, boolean mapDateToTimestamp) {
         super(yaml);
         this.mapDateToTimestamp = mapDateToTimestamp;
      }

      protected boolean shouldIgnore(SourceTypeDescriptor column) {
         return column.getDataSourceTypeName().equals("SYS.XMLTYPE");
      }

      protected SourceTypeDescriptor createTypeDescriptor(TypeMapper.AddPropertyCallback addColumnPropertyCallback, TypeMapper.InvalidMetaDataCallback invalidMetaDataCallback, Connection connection, TypeMapper.TableIdentifier table, ResultSetMetaData metaData, String columnLabel, int colIndex) throws SQLException {
         int precision = metaData.getPrecision(colIndex);
         int scale = metaData.getScale(colIndex);
         String columnClassType = metaData.getColumnClassName(colIndex);
         int colType;
         String colTypeName;
         if ("java.lang.Double".equals(columnClassType)) {
            colType = 8;
            colTypeName = SqlTypeName.FLOAT.getName();
         } else {
            colType = metaData.getColumnType(colIndex);
            colTypeName = metaData.getColumnTypeName(colIndex);
            if (colType == 2 && precision == 0) {
               precision = 38;
               scale = 6;
               if (null != addColumnPropertyCallback) {
                  addColumnPropertyCallback.addProperty(columnLabel, ColumnPropertiesProcessors.ENABLE_EXPLICIT_CAST);
               }
            } else if (this.mapDateToTimestamp && "DATE".equals(colTypeName)) {
               colTypeName = "TIMESTAMP";
            }
         }

         return new SourceTypeDescriptor(columnLabel, colType, colTypeName, colIndex, precision, scale);
      }
   }

   static class OracleSchemaFetcher extends ArpDialect.ArpSchemaFetcher {
      private static final Logger logger = LoggerFactory.getLogger(OracleDialect.OracleSchemaFetcher.class);

      public OracleSchemaFetcher(String query, JdbcPluginConfig config) {
         super(query, config, true, false);
      }

      protected long getRowCount(List<String> tablePath) {
         String sql = MessageFormat.format("SELECT NUM_ROWS FROM ALL_TAB_STATISTICS WHERE OWNER = {0} AND TABLE_NAME = {1} AND NUM_ROWS IS NOT NULL AND OBJECT_TYPE = ''TABLE'' AND STALE_STATS = ''NO''", this.getConfig().getDialect().quoteStringLiteral((String)tablePath.get(0)), this.getConfig().getDialect().quoteStringLiteral((String)tablePath.get(1)));
         Optional<Long> estimate = this.executeQueryAndGetFirstLong(sql);
         if (estimate.isPresent()) {
            return (Long)estimate.get();
         } else {
            logger.debug("Row count estimate not detected for table {}. Retrying with count query.", this.getQuotedPath(tablePath));
            return super.getRowCount(tablePath);
         }
      }

      protected CanonicalizeTablePathResponse getDatasetHandleViaGetTables(CanonicalizeTablePathRequest request, Connection connection) throws SQLException {
         StringBuilder tblBuilder = new StringBuilder(this.getQuery());
         tblBuilder.append(" AND NME = '%s'");
         String tblQuery;
         if (Strings.isNullOrEmpty(request.getCatalogOrSchema())) {
            tblQuery = String.format(tblBuilder.toString(), request.getTable());
         } else {
            tblBuilder.append(" AND SCH LIKE '%s'");
            tblQuery = String.format(tblBuilder.toString(), request.getTable(), request.getCatalogOrSchema());
         }

         Statement stmt = connection.createStatement();
         Throwable var6 = null;

         try {
            ResultSet result = stmt.executeQuery(tblQuery);
            Throwable var8 = null;

            try {
               Object var9;
               try {
                  if (result.next()) {
                     var9 = CanonicalizeTablePathResponse.newBuilder().setSchema(result.getString(2)).setTable(result.getString(3)).build();
                     return (CanonicalizeTablePathResponse)var9;
                  }
               } catch (Throwable var20) {
                  var9 = var20;
                  var8 = var20;
                  throw var20;
               }
            } finally {
               if (result != null) {
                  $closeResource(var8, result);
               }

            }
         } catch (Throwable var22) {
            var6 = var22;
            throw var22;
         } finally {
            if (stmt != null) {
               $closeResource(var6, stmt);
            }

         }

         return CanonicalizeTablePathResponse.getDefaultInstance();
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
   }

   public static enum OracleKeyWords {
      ROWNUM;
   }
}
