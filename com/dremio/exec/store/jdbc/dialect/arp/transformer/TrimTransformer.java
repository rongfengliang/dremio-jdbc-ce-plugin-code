package com.dremio.exec.store.jdbc.dialect.arp.transformer;

import com.dremio.common.dialect.arp.transformer.CallTransformer;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.DremioContext;
import com.dremio.exec.planner.sql.DynamicReturnType;
import com.dremio.exec.planner.sql.SqlOperatorImpl;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.OracleSqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlTrimFunction.Flag;
import org.apache.calcite.sql.parser.SqlParserPos;

public final class TrimTransformer extends CallTransformer {
   public static final TrimTransformer INSTANCE = new TrimTransformer();
   private static final ImmutableSet<SqlOperator> operators;

   private TrimTransformer() {
   }

   public boolean matches(RexCall call) {
      if (!this.matches(call.op)) {
         return false;
      } else {
         List<RexNode> operands = call.getOperands();
         if (operands.size() != 3) {
            return false;
         } else {
            RexNode charArg = (RexNode)operands.get(1);
            if (charArg.getKind() != SqlKind.LITERAL) {
               return false;
            } else {
               String literalValue = (String)((RexLiteral)charArg).getValueAs(String.class);
               return " ".equals(literalValue);
            }
         }
      }
   }

   public Set<SqlOperator> getCompatibleOperators() {
      return operators;
   }

   public List<SqlNode> transformSqlOperands(List<SqlNode> operands) {
      return operands.subList(2, 3);
   }

   public List<RexNode> transformRexOperands(List<RexNode> operands) {
      return operands.subList(2, 3);
   }

   public String adjustNameBasedOnOperands(String operatorName, List<RexNode> operands) {
      RexNode firstOp = (RexNode)operands.get(0);
      RexLiteral asLiteral = (RexLiteral)firstOp;
      Flag value = (Flag)asLiteral.getValueAs(Flag.class);
      switch(value) {
      case LEADING:
         return "LTRIM";
      case TRAILING:
         return "RTRIM";
      case BOTH:
      default:
         return "TRIM";
      }
   }

   public SqlOperator getAlternateOperator(RexCall call) {
      RexNode firstOp = (RexNode)call.operands.get(0);
      RexLiteral asLiteral = (RexLiteral)firstOp;
      Flag value = (Flag)asLiteral.getValueAs(Flag.class);
      switch(value) {
      case LEADING:
         return OracleSqlOperatorTable.LTRIM;
      case TRAILING:
         return OracleSqlOperatorTable.RTRIM;
      case BOTH:
      default:
         return SqlStdOperatorTable.TRIM;
      }
   }

   public Supplier<SqlNode> getAlternateCall(Supplier<SqlNode> originalNodeSupplier, DremioContext context, RexProgram program, RexCall call) {
      return () -> {
         String trimFuncName = this.adjustNameBasedOnOperands(call.getOperator().getName(), call.getOperands());
         List<SqlNode> operands = context.toSql(program, this.transformRexOperands(call.getOperands()));
         SqlOperatorImpl function = new SqlOperatorImpl(trimFuncName, operands.size(), operands.size(), true, DynamicReturnType.INSTANCE);
         return function.createCall(SqlParserPos.ZERO, operands);
      };
   }

   static {
      Builder<SqlOperator> opBuilder = ImmutableSet.builder();
      operators = opBuilder.add(SqlStdOperatorTable.TRIM).build();
   }
}
