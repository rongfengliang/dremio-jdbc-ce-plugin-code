package com.dremio.exec.store.jdbc.dialect.arp.transformer;

import com.dremio.common.dialect.arp.transformer.CallTransformer;
import com.dremio.exec.planner.sql.SqlOperatorImpl;
import com.dremio.exec.store.jdbc.EnumParameterUtils;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public final class TimeUnitFunctionTransformer extends CallTransformer {
   public static final TimeUnitFunctionTransformer INSTANCE = new TimeUnitFunctionTransformer();
   private static final ImmutableSet<SqlOperator> operators;

   private TimeUnitFunctionTransformer() {
   }

   public boolean matches(RexCall call) {
      if (!this.matches(call.op)) {
         return false;
      } else if (call.operands.isEmpty()) {
         return false;
      } else {
         RexNode firstOperand = (RexNode)call.operands.get(0);
         if (firstOperand.getKind() != SqlKind.LITERAL) {
            return false;
         } else {
            RexLiteral firstAsLiteral = (RexLiteral)firstOperand;
            if (call.op.getName().equalsIgnoreCase("DATE_TRUNC")) {
               return firstAsLiteral.getValue2() instanceof String && EnumParameterUtils.TIME_UNIT_MAPPING.containsKey(((String)firstAsLiteral.getValueAs(String.class)).toLowerCase(Locale.ROOT));
            } else if (!firstAsLiteral.getTypeName().isSpecial()) {
               return false;
            } else {
               return firstAsLiteral.getValue() instanceof TimeUnit || firstAsLiteral.getValue() instanceof TimeUnitRange;
            }
         }
      }
   }

   public Set<SqlOperator> getCompatibleOperators() {
      return operators;
   }

   public List<SqlNode> transformSqlOperands(List<SqlNode> operands) {
      return operands.subList(1, operands.size());
   }

   public List<RexNode> transformRexOperands(List<RexNode> operands) {
      return operands.subList(1, operands.size());
   }

   public String adjustNameBasedOnOperands(String operatorName, List<RexNode> operands) {
      TimeUnitRange range = EnumParameterUtils.getFirstParamAsTimeUnitRange(operands);
      return operatorName + "_" + range.toString();
   }

   static {
      Builder<SqlOperator> opBuilder = ImmutableSet.builder();
      operators = opBuilder.add(new SqlOperatorImpl("DATE_TRUNC", 2, true)).add(SqlStdOperatorTable.TIMESTAMP_ADD).add(SqlStdOperatorTable.TIMESTAMP_DIFF).add(SqlStdOperatorTable.EXTRACT).build();
   }
}
