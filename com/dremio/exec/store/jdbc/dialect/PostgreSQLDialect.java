package com.dremio.exec.store.jdbc.dialect;

import com.dremio.exec.store.jdbc.ColumnPropertiesProcessors;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpTypeMapper;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import com.dremio.exec.store.jdbc.rel2sql.PostgresRelToSqlConverter;
import com.google.common.base.Joiner;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlCollation.Coercibility;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

public final class PostgreSQLDialect extends ArpDialect {
   private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
   private static final Integer MAX_IDENTIFIER_LENGTH = 63;
   private static final boolean DISABLE_PUSH_COLLATION = Boolean.getBoolean("dremio.jdbc.postgres.push-collation.disable");
   private final SqlCollation POSTGRES_BINARY_COLLATION;
   private final ArpTypeMapper typeMapper;

   public PostgreSQLDialect(ArpYaml yaml) {
      super(yaml);
      this.POSTGRES_BINARY_COLLATION = new SqlCollation(Coercibility.NONE) {
         private static final long serialVersionUID = 1L;

         public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
            writer.keyword("COLLATE");
            writer.keyword("\"C\"");
         }
      };
      this.typeMapper = new PostgreSQLDialect.PostgreSQLTypeMapper(yaml);
   }

   protected boolean requiresAliasForFromItems() {
      return true;
   }

   public boolean supportsNestedAggregations() {
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
            case 'B':
            case 'U':
            case 'a':
            case 'b':
            case 'c':
            case 'e':
            case 'f':
            case 'n':
            case 'r':
            case 't':
            case 'u':
            case 'v':
            case 'x':
               ++index;
               break;
            case 'C':
            case 'D':
            case 'E':
            case 'F':
            case 'G':
            case 'H':
            case 'I':
            case 'J':
            case 'K':
            case 'L':
            case 'M':
            case 'N':
            case 'O':
            case 'P':
            case 'Q':
            case 'R':
            case 'S':
            case 'T':
            case 'V':
            case 'W':
            case 'X':
            case 'Y':
            case 'Z':
            case '[':
            case '\\':
            case ']':
            case '^':
            case '_':
            case '`':
            case 'd':
            case 'g':
            case 'h':
            case 'i':
            case 'j':
            case 'k':
            case 'l':
            case 'm':
            case 'o':
            case 'p':
            case 'q':
            case 's':
            case 'w':
            default:
               return false;
            }
         }
      }

      return true;
   }

   public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      if (call.getKind() == SqlKind.FLOOR && call.operandCount() == 2) {
         PostgresqlSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
      } else {
         super.unparseCall(writer, call, leftPrec, rightPrec);
      }

   }

   public TypeMapper getDataTypeMapper() {
      return this.typeMapper;
   }

   public Integer getIdentifierLengthLimit() {
      return MAX_IDENTIFIER_LENGTH;
   }

   public SqlNode getCastSpec(RelDataType type) {
      switch(type.getSqlTypeName()) {
      case DOUBLE:
         return new SqlDataTypeSpec(new SqlIdentifier("DOUBLE PRECISION", SqlParserPos.ZERO), -1, -1, (String)null, (TimeZone)null, SqlParserPos.ZERO) {
            public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
               writer.keyword("DOUBLE PRECISION");
            }
         };
      default:
         return super.getCastSpec(type);
      }
   }

   public SqlNode emulateNullDirection(SqlNode node, boolean nullsFirst, boolean desc) {
      return null;
   }

   public PostgresRelToSqlConverter getConverter() {
      return new PostgresRelToSqlConverter(this);
   }

   public ArpDialect.ArpSchemaFetcher newSchemaFetcher(JdbcPluginConfig config) {
      String query = String.format("SELECT NULL, SCH, NME FROM (SELECT TABLEOWNER CAT, SCHEMANAME SCH, TABLENAME NME from pg_catalog.pg_tables UNION ALL SELECT VIEWOWNER CAT, SCHEMANAME SCH, VIEWNAME NME FROM pg_catalog.pg_views) t WHERE UPPER(SCH) NOT IN ('PG_CATALOG', '%s')", Joiner.on("','").join(config.getHiddenSchemas()));
      return new PostgreSQLDialect.PGSchemaFetcher(query, config);
   }

   public boolean supportsFetchOffsetInSetOperand() {
      return false;
   }

   public SqlCollation getDefaultCollation(SqlKind kind) {
      if (DISABLE_PUSH_COLLATION) {
         return null;
      } else {
         switch(kind) {
         case LITERAL:
         case IDENTIFIER:
            return this.POSTGRES_BINARY_COLLATION;
         default:
            return null;
         }
      }
   }

   private static class PostgreSQLTypeMapper extends ArpTypeMapper {
      public PostgreSQLTypeMapper(ArpYaml yaml) {
         super(yaml);
      }

      protected SourceTypeDescriptor createTypeDescriptor(TypeMapper.AddPropertyCallback addColumnPropertyCallback, TypeMapper.InvalidMetaDataCallback invalidMetaDataCallback, Connection connection, TypeMapper.TableIdentifier table, ResultSetMetaData metaData, String columnLabel, int colIndex) throws SQLException {
         int colType = metaData.getColumnType(colIndex);
         int precision = metaData.getPrecision(colIndex);
         int scale = metaData.getScale(colIndex);
         if (colType == 2 && 0 == precision) {
            scale = 6;
            precision = 38;
            if (null != addColumnPropertyCallback) {
               addColumnPropertyCallback.addProperty(columnLabel, ColumnPropertiesProcessors.ENABLE_EXPLICIT_CAST);
            }
         }

         return new SourceTypeDescriptor(columnLabel, colType, metaData.getColumnTypeName(colIndex), colIndex, precision, scale);
      }

      protected SourceTypeDescriptor createTableTypeDescriptor(TypeMapper.AddPropertyCallback addColumnPropertyCallback, Connection connection, String columnName, int sourceJdbcType, String typeString, TypeMapper.TableIdentifier table, int colIndex, int precision, int scale) {
         if (precision == 131089) {
            scale = 6;
            precision = 38;
            if (null != addColumnPropertyCallback) {
               addColumnPropertyCallback.addProperty(columnName, ColumnPropertiesProcessors.ENABLE_EXPLICIT_CAST);
            }
         }

         return new TableSourceTypeDescriptor(columnName, sourceJdbcType, typeString, table.catalog, table.schema, table.catalog, colIndex, precision, scale);
      }
   }

   public static class PGSchemaFetcher extends ArpDialect.ArpSchemaFetcher {
      public PGSchemaFetcher(String query, JdbcPluginConfig config) {
         super(query, config);
      }

      protected long getRowCount(List<String> tablePath) {
         String sql = MessageFormat.format("SELECT reltuples::bigint AS EstimatedCount\nFROM pg_class\nWHERE  oid = {0}::regclass", this.config.getDialect().quoteStringLiteral(PERIOD_JOINER.join(tablePath)));
         Optional<Long> estimate = this.executeQueryAndGetFirstLong(sql);
         return estimate.isPresent() && (Long)estimate.get() != 0L ? (Long)estimate.get() : super.getRowCount(tablePath);
      }
   }
}
