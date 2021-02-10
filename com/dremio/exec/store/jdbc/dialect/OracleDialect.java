package com.dremio.exec.store.jdbc.dialect;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.store.jdbc.ColumnPropertiesProcessors;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpTypeMapper;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import com.dremio.exec.store.jdbc.rel2sql.OracleRelToSqlConverter;
import com.google.common.base.Joiner;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.TimeZone;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

public final class OracleDialect extends ArpDialect {
   private static final int ORACLE_MAX_VARCHAR_LENGTH = 4000;
   private static final Integer ORACLE_MAX_IDENTIFIER_LENGTH = 30;
   private final ArpTypeMapper typeMapper;

   public OracleDialect(ArpYaml yaml) {
      super(yaml);
      this.typeMapper = new OracleDialect.OracleTypeMapper(yaml);
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
         return new SqlDataTypeSpec(new SqlIdentifier(OracleDialect.OracleKeyWords.TIMESTAMP.toString(), SqlParserPos.ZERO), -1, -1, (String)null, (TimeZone)null, SqlParserPos.ZERO) {
            public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
               writer.keyword(OracleDialect.OracleKeyWords.TIMESTAMP.toString());
            }
         };
      default:
         return super.getCastSpec(type);
      }
   }

   public TypeMapper getDataTypeMapper() {
      return this.typeMapper;
   }

   public boolean supportsAliasedValues() {
      return false;
   }

   public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      if (call.getKind() == SqlKind.FLOOR && call.operandCount() == 2) {
         OracleSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
      } else {
         super.unparseCall(writer, call, leftPrec, rightPrec);
      }

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
      return new ArpDialect.ArpSchemaFetcher(query, config, true, false);
   }

   public boolean supportsLiteral(CompleteType type) {
      if (CompleteType.BIT.equals(type)) {
         return false;
      } else if (!CompleteType.INT.equals(type) && !CompleteType.BIGINT.equals(type)) {
         return CompleteType.DATE.equals(type) ? true : super.supportsLiteral(type);
      } else {
         return true;
      }
   }

   public boolean supportsBooleanAggregation() {
      return false;
   }

   public boolean supportsSort(boolean isCollationEmpty, boolean isOffsetEmpty) {
      return isOffsetEmpty;
   }

   public Integer getIdentifierLengthLimit() {
      return ORACLE_MAX_IDENTIFIER_LENGTH;
   }

   private static class OracleTypeMapper extends ArpTypeMapper {
      public OracleTypeMapper(ArpYaml yaml) {
         super(yaml);
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
            }
         }

         return new SourceTypeDescriptor(columnLabel, colType, colTypeName, colIndex, precision, scale);
      }
   }

   public static enum OracleKeyWords {
      ROWNUM,
      TIMESTAMP;
   }
}
