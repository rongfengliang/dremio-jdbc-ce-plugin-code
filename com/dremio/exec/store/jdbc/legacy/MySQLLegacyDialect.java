package com.dremio.exec.store.jdbc.legacy;

import com.dremio.exec.store.jdbc.rel2sql.JdbcDremioRelToSqlConverter;
import java.util.TimeZone;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlDialect.CalendarPolicy;
import org.apache.calcite.sql.SqlDialect.DatabaseProduct;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;

public final class MySQLLegacyDialect extends LegacyDialect {
   public static final MySQLLegacyDialect INSTANCE = new MySQLLegacyDialect();
   private static final int MYSQL_DECIMAL_MAX_PRECISION = 65;
   private static final int MYSQL_DECIMAL_DEFAULT_SCALE = 20;

   private MySQLLegacyDialect() {
      super(DatabaseProduct.MYSQL, DatabaseProduct.MYSQL.name(), "`", NullCollation.HIGH);
   }

   public void unparseOffsetFetch(SqlWriter writer, SqlNode offset, SqlNode fetch) {
      MysqlSqlDialect.DEFAULT.unparseOffsetFetch(writer, offset, fetch);
   }

   public SqlNode emulateNullDirection(SqlNode node, boolean nullsFirst, boolean desc) {
      return MysqlSqlDialect.DEFAULT.emulateNullDirection(node, nullsFirst, desc);
   }

   public boolean supportsAggregateFunction(SqlKind kind) {
      return MysqlSqlDialect.DEFAULT.supportsAggregateFunction(kind);
   }

   public boolean supportsNestedAggregations() {
      return false;
   }

   public CalendarPolicy getCalendarPolicy() {
      return MysqlSqlDialect.DEFAULT.getCalendarPolicy();
   }

   public SqlNode getCastSpec(RelDataType type) {
      switch(type.getSqlTypeName()) {
      case VARCHAR:
         return new SqlDataTypeSpec(new SqlIdentifier("CHAR", SqlParserPos.ZERO), type.getPrecision(), -1, (String)null, (TimeZone)null, SqlParserPos.ZERO);
      case INTEGER:
         return new SqlDataTypeSpec(new SqlIdentifier("_UNSIGNED", SqlParserPos.ZERO), type.getPrecision(), -1, (String)null, (TimeZone)null, SqlParserPos.ZERO);
      case BIGINT:
         return new SqlDataTypeSpec(new SqlIdentifier("_SIGNED INTEGER", SqlParserPos.ZERO), type.getPrecision(), -1, (String)null, (TimeZone)null, SqlParserPos.ZERO);
      case TIMESTAMP:
         return new SqlDataTypeSpec(new SqlIdentifier("_DATETIME", SqlParserPos.ZERO), -1, -1, (String)null, (TimeZone)null, SqlParserPos.ZERO);
      case DOUBLE:
         return new SqlDataTypeSpec(new SqlIdentifier("DECIMAL", SqlParserPos.ZERO), 65, 20, (String)null, (TimeZone)null, SqlParserPos.ZERO);
      default:
         return super.getCastSpec(type);
      }
   }

   public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      if (call.getKind() == SqlKind.FLOOR && call.operandCount() == 2) {
         MysqlSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
      } else {
         super.unparseCall(writer, call, leftPrec, rightPrec);
      }

   }

   public SqlNode rewriteSingleValueExpr(SqlNode aggCall) {
      return MysqlSqlDialect.DEFAULT.rewriteSingleValueExpr(aggCall);
   }

   public JdbcDremioRelToSqlConverter getConverter() {
      return new MySQLLegacyRelToSqlConverter(this);
   }

   public boolean requiresTrimOnChars() {
      return true;
   }

   public boolean supportsSort(boolean isCollationEmpty, boolean isOffsetEmpty) {
      return true;
   }

   public boolean supportsOver(RexOver over) {
      return false;
   }

   public boolean supportsOver(Window window) {
      return false;
   }
}
