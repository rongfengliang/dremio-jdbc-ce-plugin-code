package com.dremio.exec.store.jdbc.rules;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

public class ReCoalesceTransform extends RexShuttle {
   private final RelDataTypeFactory factory;
   private boolean wasTransformed = false;

   public ReCoalesceTransform(RelDataTypeFactory factory) {
      this.factory = factory;
   }

   public boolean wasTransformed() {
      return this.wasTransformed;
   }

   public RexNode visitCall(RexCall call) {
      if (call.getOperator() != SqlStdOperatorTable.CASE) {
         return super.visitCall(call);
      } else {
         List<RexNode> operands = call.getOperands();
         if (operands.size() <= 2) {
            return super.visitCall(call);
         } else if (operands.size() % 2 == 0) {
            return super.visitCall(call);
         } else {
            Iterator var3 = operands.iterator();

            RexCall secondCase;
            do {
               if (!var3.hasNext()) {
                  for(int i = 0; i < operands.size() - 1; i += 2) {
                     RexCall firstCase = (RexCall)operands.get(i);
                     secondCase = (RexCall)operands.get(i + 1);
                     RexNode firstCaseCondition = isNullIf(firstCase);
                     if (firstCaseCondition == null) {
                        return super.visitCall(call);
                     }

                     if (!checkConditionReturnNullLiteral(secondCase, firstCaseCondition)) {
                        return super.visitCall(call);
                     }
                  }

                  RexCall lastCase = (RexCall)operands.get(operands.size() - 1);
                  if (!checkConditionReturnNullLiteral(lastCase, (RexNode)null)) {
                     return super.visitCall(call);
                  }

                  List<RexNode> transformedInnerCases = this.transformInnerCases(call.getOperands());
                  RexBuilder builder = new RexBuilder(this.factory);
                  List<RexNode> nullIfOperands = (List)transformedInnerCases.stream().map((rexNode) -> {
                     RexCall transformedInnerCase = (RexCall)rexNode;
                     RexCall condition = (RexCall)transformedInnerCase.getOperands().get(0);
                     return builder.makeCall(SqlStdOperatorTable.NULLIF, new RexNode[]{(RexNode)condition.getOperands().get(0), (RexNode)condition.getOperands().get(1)});
                  }).collect(Collectors.toList());
                  return builder.makeCall(SqlStdOperatorTable.COALESCE, nullIfOperands);
               }

               RexNode operand = (RexNode)var3.next();
               if (!operand.isA(SqlKind.CASE)) {
                  return super.visitCall(call);
               }

               secondCase = (RexCall)operand;
            } while(secondCase.getOperands().size() == 3);

            return super.visitCall(call);
         }
      }
   }

   private static RexNode isNullIf(RexCall firstCase) {
      RexNode condition = (RexNode)firstCase.getOperands().get(0);
      if (condition.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
         return null;
      } else if (!(condition instanceof RexCall)) {
         return null;
      } else {
         RexCall comparison = (RexCall)condition;
         if (comparison.getOperands().size() != 2) {
            return null;
         } else {
            RexNode val = (RexNode)comparison.getOperands().get(0);
            if (!((RexNode)firstCase.getOperands().get(1)).isAlwaysFalse()) {
               return null;
            } else if (!((RexNode)firstCase.getOperands().get(2)).isA(SqlKind.IS_NOT_NULL)) {
               return null;
            } else {
               RexCall isNotNull = (RexCall)firstCase.getOperands().get(2);
               return isNotNull.getOperands().size() != 1 && ((RexNode)isNotNull.getOperands().get(0)).equals(val) ? null : condition;
            }
         }
      }
   }

   private static boolean checkConditionReturnNullLiteral(RexCall secondCase, RexNode previousCondition) {
      RexNode currentCondition = (RexNode)secondCase.getOperands().get(0);
      RexCall caseCondition;
      RexNode previousVal;
      if (previousCondition != null) {
         if (!currentCondition.equals(previousCondition)) {
            return false;
         }

         caseCondition = (RexCall)currentCondition;
         previousVal = (RexNode)caseCondition.getOperands().get(0);
      } else {
         if (currentCondition.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
            return false;
         }

         if (!(currentCondition instanceof RexCall)) {
            return false;
         }

         caseCondition = (RexCall)currentCondition;
         if (caseCondition.getOperands().size() != 2) {
            return false;
         }

         previousVal = null;
      }

      if (!RexLiteral.isNullLiteral((RexNode)secondCase.getOperands().get(1))) {
         return false;
      } else {
         RexNode val = (RexNode)caseCondition.getOperands().get(0);
         if (previousVal != null && !previousVal.equals(val)) {
            return false;
         } else {
            return !RexLiteral.isNullLiteral((RexNode)secondCase.getOperands().get(1)) ? false : ((RexNode)secondCase.getOperands().get(2)).equals(val);
         }
      }
   }

   private List<RexNode> transformInnerCases(List<RexNode> operands) {
      List<RexNode> transformedInnerCases = new ArrayList();

      for(int i = 0; i < operands.size(); ++i) {
         if (i % 2 != 0 || i == operands.size() - 1) {
            RexCall candidateCase = (RexCall)operands.get(i);
            RexNode condition = (RexNode)candidateCase.getOperands().get(0);
            RexCall comparison = (RexCall)condition;
            RexNode val1 = (RexNode)comparison.getOperands().get(0);
            ReCoalesceTransform transform1 = new ReCoalesceTransform(this.factory);
            RexNode transformedVal1 = (RexNode)val1.accept(this);
            RexNode val2 = (RexNode)comparison.getOperands().get(0);
            ReCoalesceTransform transform2 = new ReCoalesceTransform(this.factory);
            RexNode transformedVal2 = (RexNode)val2.accept(this);
            if (!transform1.wasTransformed() && !transform2.wasTransformed()) {
               transformedInnerCases.add(candidateCase);
            } else {
               this.wasTransformed = true;
               RexCall transformedCondition1 = ((RexCall)condition).clone(condition.getType(), Lists.newArrayList(new RexNode[]{transformedVal1, transformedVal2}));
               RexCall isNotNullCall = (RexCall)candidateCase.getOperands().get(1);
               RexCall transformedIsNotNull = isNotNullCall.clone(isNotNullCall.getType(), Lists.newArrayList(new RexNode[]{transformedVal1}));
               RexCall transformedFirstCase = candidateCase.clone(candidateCase.getType(), Lists.newArrayList(new RexNode[]{transformedCondition1, transformedIsNotNull}));
               transformedInnerCases.add(transformedFirstCase);
            }
         }
      }

      return transformedInnerCases;
   }
}
