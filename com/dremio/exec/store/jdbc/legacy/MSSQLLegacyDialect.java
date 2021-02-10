package com.dremio.exec.store.jdbc.legacy;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.planner.sql.handlers.OverUtils;
import com.dremio.exec.store.jdbc.dialect.SqlSelectExtraKeyword;
import com.dremio.exec.store.jdbc.rel2sql.JdbcDremioRelToSqlConverter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.UnmodifiableIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import org.apache.calcite.config.NullCollation;
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
import org.apache.calcite.sql.SqlDialect.DatabaseProduct;
import org.apache.calcite.sql.SqlWriter.Frame;
import org.apache.calcite.sql.SqlWriter.FrameTypeEnum;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

public final class MSSQLLegacyDialect extends LegacyDialect {
   public static final MSSQLLegacyDialect INSTANCE = new MSSQLLegacyDialect();
   public static final Set<SqlAggFunction> SUPPORTED_WINDOW_AGG_CALLS;
   private static final int MSSQL_MAX_VARCHAR_LENGTH = 8000;
   private static final String DOUBLE_PRECISION = "DOUBLE PRECISION";
   private static final boolean DISABLE_PUSH_COLLATION;
   private final SqlCollation MSSQL_BINARY_COLLATION;

   private MSSQLLegacyDialect() {
      super(DatabaseProduct.MSSQL, DatabaseProduct.MSSQL.name(), "[", NullCollation.HIGH);
      this.MSSQL_BINARY_COLLATION = new SqlCollation(Coercibility.NONE) {
         private static final long serialVersionUID = 1L;

         public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
            writer.keyword("COLLATE");
            writer.keyword("Latin1_General_BIN2");
         }
      };
   }

   public boolean useTimestampAddInsteadOfDatetimePlus() {
      return true;
   }

   public void unparseOffsetFetch(SqlWriter writer, SqlNode offset, SqlNode fetch) {
      writer.newlineAndIndent();
      Frame offsetFrame = writer.startList(FrameTypeEnum.OFFSET);
      writer.keyword("OFFSET");
      if (offset == null) {
         writer.literal("0");
      } else {
         offset.unparse(writer, -1, -1);
      }

      writer.keyword("ROWS");
      writer.endList(offsetFrame);
      if (fetch != null) {
         writer.newlineAndIndent();
         Frame fetchFrame = writer.startList(FrameTypeEnum.FETCH);
         writer.keyword("FETCH");
         writer.keyword("NEXT");
         fetch.unparse(writer, -1, -1);
         writer.keyword("ROWS");
         writer.keyword("ONLY");
         writer.endList(fetchFrame);
      }

   }

   public void unparseDateTimeLiteral(SqlWriter writer, SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec) {
      writer.literal("'" + literal.toFormattedString() + "'");
   }

   public boolean supportsNestedAggregations() {
      return false;
   }

   public boolean supportsFunction(SqlOperator operator, RelDataType type, List<RelDataType> paramTypes) {
      return operator != SqlStdOperatorTable.DATETIME_MINUS && operator != SqlStdOperatorTable.DATETIME_PLUS ? super.supportsFunction(operator, type, paramTypes) : false;
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

   public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      SqlOperator op = call.getOperator();
      ArrayList modifiedOperands;
      if (op == SqlStdOperatorTable.TRUNCATE) {
         modifiedOperands = Lists.newArrayList();
         modifiedOperands.addAll(call.getOperandList());
         modifiedOperands.add(SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO));
         super.unparseCall(writer, SqlStdOperatorTable.ROUND.createCall(new SqlNodeList(modifiedOperands, SqlParserPos.ZERO)), leftPrec, rightPrec);
      } else if (op == SqlStdOperatorTable.TIMESTAMP_DIFF) {
         super.unparseCall(writer, DATEDIFF.createCall(new SqlNodeList(call.getOperandList(), SqlParserPos.ZERO)), leftPrec, rightPrec);
      } else if (op == SqlStdOperatorTable.SUBSTRING && call.operandCount() == 2) {
         modifiedOperands = Lists.newArrayList();
         modifiedOperands.addAll(call.getOperandList());
         modifiedOperands.add(SqlLiteral.createExactNumeric(String.valueOf(Long.MAX_VALUE), SqlParserPos.ZERO));
         MssqlSqlDialect.DEFAULT.unparseCall(writer, SqlStdOperatorTable.SUBSTRING.createCall(new SqlNodeList(modifiedOperands, SqlParserPos.ZERO)), leftPrec, rightPrec);
      } else if (call instanceof SqlSelect) {
         SqlSelect select = (SqlSelect)call;
         if (select.getFetch() != null && (select.getOffset() == null || (Long)((SqlLiteral)select.getOffset()).getValueAs(Long.class) == 0L)) {
            SqlNodeList keywords = new SqlNodeList(SqlParserPos.ZERO);
            if (select.getModifierNode(SqlSelectKeyword.DISTINCT) != null) {
               keywords.add(select.getModifierNode(SqlSelectKeyword.DISTINCT));
            } else if (select.getModifierNode(SqlSelectKeyword.ALL) != null) {
               keywords.add(select.getModifierNode(SqlSelectKeyword.ALL));
            }

            keywords.add(SqlSelectExtraKeyword.TOP.symbol(SqlParserPos.ZERO));
            keywords.add(select.getFetch());
            SqlSelect modifiedSelect = SqlSelectOperator.INSTANCE.createCall(keywords, select.getSelectList(), select.getFrom(), select.getWhere(), select.getGroup(), select.getHaving(), select.getWindowList(), select.getOrderList(), (SqlNode)null, (SqlNode)null, SqlParserPos.ZERO);
            super.unparseCall(writer, modifiedSelect, leftPrec, rightPrec);
         } else {
            super.unparseCall(writer, call, leftPrec, rightPrec);
         }
      } else if (op != SqlStdOperatorTable.SUBSTRING && (call.getKind() != SqlKind.FLOOR || call.operandCount() != 2) && op != SqlStdOperatorTable.TIMESTAMP_ADD) {
         super.unparseCall(writer, call, leftPrec, rightPrec);
      } else {
         MssqlSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
      }

   }

   public JdbcDremioRelToSqlConverter getConverter() {
      return new MSSQLLegacyRelToSqlConverter(this);
   }

   public boolean requiresTrimOnChars() {
      return true;
   }

   public boolean supportsLiteral(CompleteType type) {
      return CompleteType.BIT.equals(type) ? false : super.supportsLiteral(type);
   }

   public boolean supportsBooleanAggregation() {
      return false;
   }

   public boolean supportsSort(boolean isCollationEmpty, boolean isOffsetEmpty) {
      return !isCollationEmpty || isOffsetEmpty;
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
            return this.MSSQL_BINARY_COLLATION;
         default:
            return null;
         }
      }
   }

   static {
      SUPPORTED_WINDOW_AGG_CALLS = ImmutableSet.of(SqlStdOperatorTable.COUNT, SqlStdOperatorTable.LAST_VALUE, SqlStdOperatorTable.FIRST_VALUE);
      DISABLE_PUSH_COLLATION = Boolean.getBoolean("dremio.jdbc.mssql.push-collation.disable");
   }
}
