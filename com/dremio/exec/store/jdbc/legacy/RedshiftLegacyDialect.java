package com.dremio.exec.store.jdbc.legacy;

import com.dremio.exec.store.jdbc.rel2sql.JdbcDremioRelToSqlConverter;
import com.dremio.exec.store.jdbc.rel2sql.JdbcDremioRelToSqlConverterBase;
import com.google.common.collect.UnmodifiableIterator;
import java.util.List;
import java.util.TimeZone;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.core.Window.Group;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexOver;
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
import org.apache.calcite.sql.dialect.MssqlSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeInference;

public final class RedshiftLegacyDialect extends LegacyDialect {
   public static final RedshiftLegacyDialect INSTANCE = new RedshiftLegacyDialect();
   private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
   private static final SqlFunction LOG;
   private static final SqlFunction TRUNC;

   private RedshiftLegacyDialect() {
      super(DatabaseProduct.REDSHIFT, DatabaseProduct.REDSHIFT.name(), "\"", NullCollation.HIGH);
   }

   public boolean supportsNestedAggregations() {
      return false;
   }

   public boolean supportsFunction(SqlOperator operator, RelDataType type, List<RelDataType> paramTypes) {
      return operator != SqlStdOperatorTable.DATETIME_MINUS && operator != SqlStdOperatorTable.DATETIME_PLUS ? super.supportsFunction(operator, type, paramTypes) : false;
   }

   public boolean useTimestampAddInsteadOfDatetimePlus() {
      return true;
   }

   public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      SqlOperator op = call.getOperator();
      if (op == SqlStdOperatorTable.LOG10) {
         super.unparseCall(writer, LOG.createCall(new SqlNodeList(call.getOperandList(), SqlParserPos.ZERO)), leftPrec, rightPrec);
      } else if (op == SqlStdOperatorTable.TRUNCATE) {
         super.unparseCall(writer, TRUNC.createCall(new SqlNodeList(call.getOperandList(), SqlParserPos.ZERO)), leftPrec, rightPrec);
      } else if (op == SqlStdOperatorTable.TIMESTAMP_DIFF) {
         super.unparseCall(writer, DATEDIFF.createCall(new SqlNodeList(call.getOperandList(), SqlParserPos.ZERO)), leftPrec, rightPrec);
      } else if (op == SqlStdOperatorTable.TIMESTAMP_ADD) {
         MssqlSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
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

   public boolean supportsFetchOffsetInSetOperand() {
      return false;
   }

   public boolean removeDefaultWindowFrame(RexOver over) {
      return SqlKind.AGGREGATE.contains(over.getAggOperator().getKind());
   }

   public boolean supportsOver(Window window) {
      if (window.groups.isEmpty()) {
         return false;
      } else {
         UnmodifiableIterator var2 = window.groups.iterator();

         Group group;
         do {
            if (!var2.hasNext()) {
               return true;
            }

            group = (Group)var2.next();
         } while(group.isRows);

         return false;
      }
   }

   public boolean supportsOver(RexOver over) {
      return over.getWindow() != null && over.getWindow().isRows();
   }

   static {
      LOG = new SqlFunction("LOG", SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE_NULLABLE, (SqlOperandTypeInference)null, OperandTypes.NUMERIC, SqlFunctionCategory.NUMERIC);
      TRUNC = new SqlFunction("TRUNC", SqlKind.OTHER_FUNCTION, ReturnTypes.ARG0_NULLABLE, (SqlOperandTypeInference)null, OperandTypes.NUMERIC_OPTIONAL_INTEGER, SqlFunctionCategory.NUMERIC);
   }
}
