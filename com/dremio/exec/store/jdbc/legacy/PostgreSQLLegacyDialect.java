package com.dremio.exec.store.jdbc.legacy;

import com.dremio.exec.store.jdbc.rel2sql.JdbcDremioRelToSqlConverter;
import com.dremio.exec.store.jdbc.rel2sql.JdbcDremioRelToSqlConverterBase;
import java.util.List;
import java.util.TimeZone;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlDialect.DatabaseProduct;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeInference;

public final class PostgreSQLLegacyDialect extends LegacyDialect {
   public static final PostgreSQLLegacyDialect INSTANCE = new PostgreSQLLegacyDialect();
   private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
   private static final SqlFunction LOG;
   private static final SqlFunction TRUNC;

   private PostgreSQLLegacyDialect() {
      super(DatabaseProduct.POSTGRESQL, DatabaseProduct.POSTGRESQL.name(), "\"", NullCollation.HIGH);
   }

   protected boolean requiresAliasForFromItems() {
      return true;
   }

   public boolean supportsNestedAggregations() {
      return false;
   }

   public boolean supportsFunction(SqlOperator operator, RelDataType type, List<RelDataType> paramTypes) {
      return operator == SqlStdOperatorTable.TIMESTAMP_DIFF ? false : super.supportsFunction(operator, type, paramTypes);
   }

   public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      SqlOperator op = call.getOperator();
      if (op == SqlStdOperatorTable.LOG10) {
         super.unparseCall(writer, LOG.createCall(new SqlNodeList(call.getOperandList(), SqlParserPos.ZERO)), leftPrec, rightPrec);
      } else if (op == SqlStdOperatorTable.TRUNCATE) {
         super.unparseCall(writer, TRUNC.createCall(new SqlNodeList(call.getOperandList(), SqlParserPos.ZERO)), leftPrec, rightPrec);
      } else if (call.getKind() == SqlKind.FLOOR && call.operandCount() == 2) {
         PostgresqlSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
      } else {
         super.unparseCall(writer, call, leftPrec, rightPrec);
      }

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

   public JdbcDremioRelToSqlConverter getConverter() {
      return new JdbcDremioRelToSqlConverterBase(this);
   }

   public boolean supportsBooleanAggregation() {
      return false;
   }

   public boolean supportsSort(boolean isCollationEmpty, boolean isOffsetEmpty) {
      return true;
   }

   public boolean supportsFetchOffsetInSetOperand() {
      return false;
   }

   static {
      LOG = new SqlFunction("LOG", SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE_NULLABLE, (SqlOperandTypeInference)null, OperandTypes.NUMERIC, SqlFunctionCategory.NUMERIC);
      TRUNC = new SqlFunction("TRUNC", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0_NULLABLE, (SqlOperandTypeInference)null, OperandTypes.NUMERIC_OPTIONAL_INTEGER, SqlFunctionCategory.NUMERIC);
   }
}
