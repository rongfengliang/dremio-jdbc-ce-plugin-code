package com.dremio.exec.store.jdbc.dialect;

import com.dremio.exec.planner.sql.handlers.OverUtils;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcSchemaFetcher;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import com.dremio.exec.store.jdbc.rel.JdbcSort;
import com.dremio.exec.store.jdbc.rel2sql.MSSQLRelToSqlConverter;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.UnmodifiableIterator;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.core.Window.Group;
import org.apache.calcite.rel.core.Window.RexWinAggCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSelectOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlCollation.Coercibility;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MSSQLDialect extends ArpDialect {
   public static final Set<SqlAggFunction> SUPPORTED_WINDOW_AGG_CALLS;
   private static final int MSSQL_MAX_VARCHAR_LENGTH = 8000;
   private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
   private static final boolean DISABLE_PUSH_COLLATION;
   private static final SqlCollation MSSQL_BINARY_COLLATION;

   public MSSQLDialect(ArpYaml yaml) {
      super(yaml);
   }

   public void unparseDateTimeLiteral(SqlWriter writer, SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec) {
      writer.literal("'" + literal.toFormattedString() + "'");
   }

   protected SqlNode emulateNullDirectionWithIsNull(SqlNode node, boolean nullsFirst, boolean desc) {
      return this.emulateNullDirectionWithCaseIsNull(node, nullsFirst, desc);
   }

   public boolean supportsSort(Sort e) {
      boolean hasOrderBy = e.getCollation() != null && !e.getCollation().getFieldCollations().isEmpty();
      return !hasOrderBy && !JdbcSort.isOffsetEmpty(e) ? false : super.supportsSort(e);
   }

   public SqlNode getCastSpec(RelDataType type) {
      switch(type.getSqlTypeName()) {
      case VARCHAR:
         if (type.getPrecision() <= 8000 && type.getPrecision() != -1) {
            return getVarcharWithPrecision(this, type, type.getPrecision());
         }

         return getVarcharWithPrecision(this, type, 8000);
      case TIMESTAMP:
         return new SqlDataTypeSpec(new SqlIdentifier("DATETIME2", SqlParserPos.ZERO), -1, -1, (String)null, (TimeZone)null, SqlParserPos.ZERO);
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

   public boolean useTimestampAddInsteadOfDatetimePlus() {
      return true;
   }

   public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      SqlOperator op = call.getOperator();
      if (op == SqlStdOperatorTable.TIMESTAMP_DIFF) {
         super.unparseCall(writer, DATEDIFF.createCall(new SqlNodeList(call.getOperandList(), SqlParserPos.ZERO)), leftPrec, rightPrec);
      } else if (call instanceof SqlSelect) {
         SqlSelect select = (SqlSelect)call;
         if (select.getFetch() == null || select.getOffset() != null && (Long)((SqlLiteral)select.getOffset()).getValueAs(Long.class) != 0L) {
            super.unparseCall(writer, call, leftPrec, rightPrec);
         } else {
            SqlNodeList keywords = new SqlNodeList(SqlParserPos.ZERO);
            if (select.getModifierNode(SqlSelectKeyword.DISTINCT) != null) {
               keywords.add(select.getModifierNode(SqlSelectKeyword.DISTINCT));
            } else if (select.getModifierNode(SqlSelectKeyword.ALL) != null) {
               keywords.add(select.getModifierNode(SqlSelectKeyword.ALL));
            }

            keywords.add(SqlSelectExtraKeyword.TOP.symbol(SqlParserPos.ZERO));
            keywords.add(select.getFetch());
            SqlSelect modifiedSelect = SqlSelectOperator.INSTANCE.createCall(keywords, select.getSelectList(), select.getFrom(), select.getWhere(), select.getGroup(), select.getHaving(), select.getWindowList(), select.getOrderList(), (SqlNode)null, (SqlNode)null, (SqlNodeList)null, SqlParserPos.ZERO);
            super.unparseCall(writer, modifiedSelect, leftPrec, rightPrec);
         }
      } else {
         super.unparseCall(writer, call, leftPrec, rightPrec);
      }

   }

   public MSSQLRelToSqlConverter getConverter() {
      return new MSSQLRelToSqlConverter(this);
   }

   public JdbcSchemaFetcher newSchemaFetcher(JdbcPluginConfig config) {
      return new MSSQLDialect.MSSQLSchemaFetcher(config);
   }

   public boolean requiresTrimOnChars() {
      return true;
   }

   public boolean supportsOver(Window window) {
      UnmodifiableIterator var2 = window.groups.iterator();

      while(var2.hasNext()) {
         Group group = (Group)var2.next();
         boolean notBounded = group.lowerBound == null && group.upperBound == null;
         UnmodifiableIterator var5 = group.aggCalls.iterator();

         while(var5.hasNext()) {
            RexWinAggCall aggCall = (RexWinAggCall)var5.next();
            SqlAggFunction operator = (SqlAggFunction)aggCall.getOperator();
            boolean hasEmptyFrame = notBounded || OverUtils.hasDefaultFrame(operator, group.isRows, group.lowerBound, group.upperBound, group.orderKeys.getFieldCollations().size());
            if (!hasEmptyFrame && !SUPPORTED_WINDOW_AGG_CALLS.contains(operator)) {
               return false;
            }
         }
      }

      return true;
   }

   public boolean supportsOver(RexOver over) {
      boolean hasEmptyFrame = over.getWindow().getLowerBound() == null && over.getWindow().getUpperBound() == null || OverUtils.hasDefaultFrame(over);
      return hasEmptyFrame ? true : SUPPORTED_WINDOW_AGG_CALLS.contains(over.getAggOperator());
   }

   public SqlCollation getDefaultCollation(SqlKind kind) {
      if (DISABLE_PUSH_COLLATION) {
         return null;
      } else {
         switch(kind) {
         case LITERAL:
         case IDENTIFIER:
            return MSSQL_BINARY_COLLATION;
         default:
            return null;
         }
      }
   }

   static {
      SUPPORTED_WINDOW_AGG_CALLS = ImmutableSet.of(SqlStdOperatorTable.COUNT, SqlStdOperatorTable.LAST_VALUE, SqlStdOperatorTable.FIRST_VALUE);
      DISABLE_PUSH_COLLATION = Boolean.getBoolean("dremio.jdbc.mssql.push-collation.disable");
      MSSQL_BINARY_COLLATION = new SqlCollation(Coercibility.NONE) {
         private static final long serialVersionUID = 1L;

         public void unparse(SqlWriter writer) {
            writer.keyword("COLLATE");
            writer.keyword("Latin1_General_BIN2");
         }
      };
   }

   private static final class MSSQLSchemaFetcher extends ArpDialect.ArpSchemaFetcher {
      private static final Logger logger = LoggerFactory.getLogger(MSSQLDialect.MSSQLSchemaFetcher.class);

      private MSSQLSchemaFetcher(JdbcPluginConfig config) {
         super("SELECT * FROM @AllTables WHERE 1=1", config, true, false);
      }

      protected String filterQuery(String query, DatabaseMetaData metaData) throws SQLException {
         return "SET NOCOUNT ON;\nDECLARE @dbname nvarchar(128), @litdbname nvarchar(128)\nDECLARE @AllTables table (CAT sysname, SCH sysname, NME sysname)\nBEGIN\n    DECLARE DBCursor CURSOR FOR SELECT name FROM sys.sysdatabases WHERE HAS_DBACCESS(name) = 1\n    OPEN DBCursor\n    FETCH NEXT FROM DBCursor INTO @dbname\n    WHILE @@FETCH_STATUS = 0\n    BEGIN\n        IF @dbname <> 'tempdb' BEGIN\n            SET @litdbname = REPLACE(@dbname, '''', '''''')\n            INSERT INTO @AllTables (CAT, SCH, NME) \n                EXEC('SELECT ''' + @litdbname + ''' CAT, SCH, NME FROM (SELECT s.name SCH, t.name AS NME FROM [' + @dbname + '].sys.tables t with (nolock) INNER JOIN [' + @dbname + '].sys.schemas s with (nolock) ON t.schema_id=s.schema_id UNION ALL SELECT s.name SCH, v.name AS NME FROM [' + @dbname + '].sys.views v with (nolock) INNER JOIN [' + @dbname + '].sys.schemas s with (nolock) ON v.schema_id=s.schema_id) t  WHERE SCH NOT IN (''" + String.format("%s", Joiner.on("'',''").join(this.getConfig().getHiddenSchemas())) + "'')')\n        END\n        FETCH NEXT FROM DBCursor INTO @dbname\n    END\n    CLOSE DBCursor\n    DEALLOCATE DBCursor\n" + super.filterQuery(query, metaData) + "\nEND;\nSET NOCOUNT OFF;";
      }

      public long getRowCount(List<String> tablePath) {
         String sql = MessageFormat.format("SELECT p.rows \nFROM {0}.sys.tables AS tbl\nINNER JOIN {0}.sys.indexes AS idx ON idx.object_id = tbl.object_id and idx.index_id < 2\nINNER JOIN {0}.sys.partitions AS p ON p.object_id=tbl.object_id\nAND p.index_id=idx.index_id\nWHERE ((tbl.name={2}\nAND SCHEMA_NAME(tbl.schema_id)={1}))", this.getConfig().getDialect().quoteIdentifier((String)tablePath.get(0)), this.getConfig().getDialect().quoteStringLiteral((String)tablePath.get(1)), this.getConfig().getDialect().quoteStringLiteral((String)tablePath.get(2)));
         String quotedPath = this.getQuotedPath(tablePath);
         Optional<Long> estimate = this.executeQueryAndGetFirstLong(sql);
         if (estimate.isPresent()) {
            return (Long)estimate.get();
         } else {
            logger.debug("Row count estimate not detected for table {}. Retrying with count_big query.", quotedPath);
            Optional<Long> fallbackEstimate = this.executeQueryAndGetFirstLong("SELECT COUNT_BIG(*) FROM " + quotedPath);
            return (Long)fallbackEstimate.orElse(1000000000L);
         }
      }

      // $FF: synthetic method
      MSSQLSchemaFetcher(JdbcPluginConfig x0, Object x1) {
         this(x0);
      }
   }
}
