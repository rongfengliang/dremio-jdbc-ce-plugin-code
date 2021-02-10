package com.dremio.exec.store.jdbc.legacy;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.store.jdbc.rel2sql.JdbcDremioRelToSqlConverter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
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
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlDialect.DatabaseProduct;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeInference;

public final class OracleLegacyDialect extends LegacyDialect {
   public static final OracleLegacyDialect INSTANCE = new OracleLegacyDialect();
   private static final int ORACLE_MAX_VARCHAR_LENGTH = 4000;
   private static final SqlFunction LOG;
   private static final SqlFunction TRUNC;

   private OracleLegacyDialect() {
      super(DatabaseProduct.ORACLE, DatabaseProduct.ORACLE.name(), "\"", NullCollation.HIGH);
   }

   public SqlNode getCastSpec(RelDataType type) {
      switch(type.getSqlTypeName()) {
      case VARCHAR:
         if (type.getPrecision() <= 4000 && type.getPrecision() != -1) {
            return getVarcharWithPrecision(this, type, type.getPrecision());
         }

         return getVarcharWithPrecision(this, type, 4000);
      case DOUBLE:
         return new SqlDataTypeSpec(new SqlIdentifier(OracleLegacyDialect.OracleKeyWords.NUMBER.toString(), SqlParserPos.ZERO), -1, -1, (String)null, (TimeZone)null, SqlParserPos.ZERO) {
            public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
               writer.keyword(OracleLegacyDialect.OracleKeyWords.NUMBER.toString());
            }
         };
      default:
         return super.getCastSpec(type);
      }
   }

   public boolean supportsAliasedValues() {
      return false;
   }

   public boolean supportsFunction(SqlOperator operator, RelDataType type, List<RelDataType> paramTypes) {
      return operator == SqlStdOperatorTable.TIMESTAMP_DIFF ? false : super.supportsFunction(operator, type, paramTypes);
   }

   public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      SqlOperator op = call.getOperator();
      if (op == SqlStdOperatorTable.LOG10) {
         List<SqlNode> modifiedOperands = Lists.newArrayList();
         modifiedOperands.add(SqlLiteral.createExactNumeric("10.0", SqlParserPos.ZERO));
         modifiedOperands.addAll(call.getOperandList());
         super.unparseCall(writer, LOG.createCall(new SqlNodeList(modifiedOperands, SqlParserPos.ZERO)), leftPrec, rightPrec);
      } else if ((call.getKind() != SqlKind.FLOOR || call.operandCount() != 2) && op != SqlStdOperatorTable.SUBSTRING) {
         if (op == SqlStdOperatorTable.TRUNCATE) {
            super.unparseCall(writer, TRUNC.createCall(new SqlNodeList(call.getOperandList(), SqlParserPos.ZERO)), leftPrec, rightPrec);
         } else {
            super.unparseCall(writer, call, leftPrec, rightPrec);
         }
      } else {
         OracleSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
      }

   }

   protected boolean allowsAs() {
      return false;
   }

   public JdbcDremioRelToSqlConverter getConverter() {
      return new OracleLegacyRelToSqlConverter(this);
   }

   public boolean supportsLiteral(CompleteType type) {
      return !CompleteType.BIT.equals(type);
   }

   public boolean supportsBooleanAggregation() {
      return false;
   }

   public boolean supportsSort(boolean isCollationEmpty, boolean isOffsetEmpty) {
      return isOffsetEmpty;
   }

   static {
      LOG = new SqlFunction("LOG", SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE_NULLABLE, (SqlOperandTypeInference)null, OperandTypes.NUMERIC_NUMERIC, SqlFunctionCategory.NUMERIC);
      TRUNC = new SqlFunction("TRUNC", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0_NULLABLE, (SqlOperandTypeInference)null, OperandTypes.NUMERIC_OPTIONAL_INTEGER, SqlFunctionCategory.NUMERIC);
   }

   public static enum OracleKeyWords {
      NUMBER("NUMBER"),
      ROWNUM("ROWNUM");

      private final String name;

      private OracleKeyWords(String name) {
         this.name = (String)Preconditions.checkNotNull(name);
      }

      public String toString() {
         return this.name;
      }
   }
}
