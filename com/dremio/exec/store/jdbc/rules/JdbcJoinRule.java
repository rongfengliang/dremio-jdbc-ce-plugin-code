package com.dremio.exec.store.jdbc.rules;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.logical.JoinRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.jdbc.dialect.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.dialect.SourceTypeDescriptor;
import com.dremio.exec.store.jdbc.rel.JdbcJoin;
import com.google.common.annotations.VisibleForTesting;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcJoinRule extends JdbcBinaryConverterRule {
   private static final Logger logger = LoggerFactory.getLogger(JdbcJoinRule.class);
   public static final JdbcJoinRule CALCITE_INSTANCE = new JdbcJoinRule(LogicalJoin.class, "JdbcJoinRuleCrel");
   public static final JdbcJoinRule LOGICAL_INSTANCE = new JdbcJoinRule(JoinRel.class, "JdbcJoinRuleDrel");

   private JdbcJoinRule(Class<? extends Join> clazz, String name) {
      super(clazz, name);
   }

   public boolean matchImpl(RelOptRuleCall call) {
      Join join = (Join)call.rel(0);
      JdbcCrel leftChild = (JdbcCrel)call.rel(1);
      StoragePluginId pluginId = leftChild.getPluginId();
      if (pluginId == null) {
         return true;
      } else {
         JdbcDremioSqlDialect dialect = getDialect(pluginId);
         JoinRelType joinType = join.getJoinType();
         boolean isJoinTypeSupported;
         if (joinType == JoinRelType.INNER && (join.getCondition() == null || join.getCondition().isAlwaysTrue())) {
            logger.debug("Checking if CROSS JOIN is supported by dialect.");
            isJoinTypeSupported = dialect.supportsJoin(JoinType.CROSS);
         } else {
            switch(joinType) {
            case INNER:
               isJoinTypeSupported = dialect.supportsJoin(JoinType.INNER);
               break;
            case LEFT:
               isJoinTypeSupported = dialect.supportsJoin(JoinType.LEFT);
               break;
            case RIGHT:
               isJoinTypeSupported = dialect.supportsJoin(JoinType.RIGHT);
               break;
            case FULL:
            default:
               isJoinTypeSupported = dialect.supportsJoin(JoinType.FULL);
            }
         }

         if (isJoinTypeSupported) {
            logger.debug("Join type is supported.");
            return true;
         } else {
            logger.debug("Join type is not supported.");
            return false;
         }
      }
   }

   public RelNode convert(RelNode rel, JdbcCrel left, JdbcCrel right, StoragePluginId pluginId) {
      Join join = (Join)rel;
      RexNode originalCondition = join.getCondition();
      if (!satisfiesPrecondition(originalCondition)) {
         assert false : String.format("'%s' is not supported", originalCondition);

         return null;
      } else {
         JdbcDremioSqlDialect dialect = null == pluginId ? null : getDialect(pluginId);
         Pair<Boolean, Boolean> canJoin = canJoinOnCondition(originalCondition, dialect);
         if (!(Boolean)canJoin.left) {
            return tryFilterOnJoin(join, left, right, pluginId);
         } else {
            RexNode newCondition;
            if ((Boolean)canJoin.right && null != pluginId && !dialect.supportsLiteral(CompleteType.BIT)) {
               RexBuilder builder = rel.getCluster().getRexBuilder();
               final RexNode falseCall = builder.makeCall(SqlStdOperatorTable.GREATER_THAN, new RexNode[]{builder.makeBigintLiteral(BigDecimal.ZERO), builder.makeBigintLiteral(BigDecimal.ONE)});
               final RexNode trueCall = builder.makeCall(SqlStdOperatorTable.GREATER_THAN, new RexNode[]{builder.makeBigintLiteral(BigDecimal.ONE), builder.makeBigintLiteral(BigDecimal.ZERO)});
               RexShuttle shuttle = new RexShuttle() {
                  public RexNode visitLiteral(RexLiteral literal) {
                     if (literal.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
                        return literal;
                     } else {
                        boolean value = RexLiteral.booleanValue(literal);
                        return value ? trueCall : falseCall;
                     }
                  }
               };
               newCondition = shuttle.apply(originalCondition);
            } else {
               newCondition = originalCondition;
            }

            return new JdbcJoin(join.getCluster(), join.getTraitSet().replace(Rel.LOGICAL), left.getInput(), right.getInput(), newCondition, join.getVariablesSet(), join.getJoinType(), pluginId);
         }
      }
   }

   @VisibleForTesting
   static boolean satisfiesPrecondition(RexNode rexNode) {
      return rexNode.isAlwaysTrue() || rexNode.isAlwaysFalse() || rexNode instanceof RexCall;
   }

   @VisibleForTesting
   static Pair<Boolean, Boolean> canJoinOnCondition(RexNode node, JdbcDremioSqlDialect dialect) {
      Iterator var3;
      RexNode operand;
      Pair opResult;
      boolean foundBoolean;
      switch(node.getKind()) {
      case LITERAL:
         RexLiteral literal = (RexLiteral)node;
         switch(literal.getTypeName().getFamily()) {
         case BOOLEAN:
            return Pair.of(true, true);
         case CHARACTER:
         case NUMERIC:
         case EXACT_NUMERIC:
         case APPROXIMATE_NUMERIC:
         case INTERVAL_YEAR_MONTH:
         case INTERVAL_DAY_TIME:
         case DATE:
         case TIME:
         case TIMESTAMP:
            if (null != dialect && !dialect.supportsLiteral(SourceTypeDescriptor.getType(literal.getType()))) {
               return Pair.of(false, false);
            }

            return Pair.of(true, false);
         case ANY:
         case NULL:
            if (literal.getTypeName() == SqlTypeName.NULL) {
               return Pair.of(true, false);
            }
         default:
            return Pair.of(false, false);
         }
      case AND:
      case OR:
         assert node instanceof RexCall;

         foundBoolean = false;
         var3 = ((RexCall)node).getOperands().iterator();

         while(var3.hasNext()) {
            operand = (RexNode)var3.next();
            if (!satisfiesPrecondition(operand) && !operand.getKind().equals(SqlKind.INPUT_REF) && !operand.getType().getSqlTypeName().equals(SqlTypeName.BOOLEAN) && !dialect.supportsLiteral(CompleteType.BIT)) {
               return Pair.of(false, foundBoolean);
            }

            opResult = canJoinOnCondition(operand, dialect);
            if (!(Boolean)opResult.left) {
               return Pair.of(false, (Boolean)opResult.right);
            }

            if ((Boolean)opResult.right) {
               foundBoolean = true;
            }
         }

         return Pair.of(true, foundBoolean);
      case IS_NOT_DISTINCT_FROM:
      case NOT_EQUALS:
      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL:
      case EQUALS:
         assert node instanceof RexCall;

         foundBoolean = false;
         var3 = ((RexCall)node).getOperands().iterator();

         while(var3.hasNext()) {
            operand = (RexNode)var3.next();
            opResult = canJoinOnCondition(operand, dialect);
            if (!(Boolean)opResult.left) {
               return Pair.of(false, (Boolean)opResult.right);
            }

            if ((Boolean)opResult.right) {
               foundBoolean = true;
            }
         }

         return Pair.of(true, foundBoolean);
      case IS_NULL:
      case IS_NOT_NULL:
         List<RexNode> operands = ((RexCall)node).getOperands();
         return Pair.of(operands.size() == 1 && operands.get(0) instanceof RexInputRef, false);
      case INPUT_REF:
         return Pair.of(true, false);
      default:
         if (null != dialect && node instanceof RexCall) {
            RexCall call = (RexCall)node;
            if (call.getOperator() instanceof SqlFunction && ((SqlFunction)call.getOperator()).getFunctionType().isFunction()) {
               if (!dialect.supportsFunction(call.getOperator(), call.getType(), (List)call.getOperands().stream().map(RexNode::getType).collect(Collectors.toList()))) {
                  return Pair.of(false, false);
               }

               boolean foundBoolean = false;
               Iterator var11 = ((RexCall)node).getOperands().iterator();

               while(var11.hasNext()) {
                  RexNode operand = (RexNode)var11.next();
                  Pair<Boolean, Boolean> opResult = canJoinOnCondition(operand, dialect);
                  if (!(Boolean)opResult.left) {
                     return Pair.of(false, (Boolean)opResult.right);
                  }

                  if ((Boolean)opResult.right) {
                     foundBoolean = true;
                  }
               }

               return Pair.of(true, foundBoolean);
            }
         }

         return Pair.of(false, false);
      }
   }

   private static RelNode tryFilterOnJoin(Join join, JdbcCrel left, JdbcCrel right, StoragePluginId pluginId) {
      if (join.getJoinType() != JoinRelType.INNER) {
         return null;
      } else {
         RelOptCluster cluster = join.getCluster();
         RexBuilder builder = cluster.getRexBuilder();
         RexNode trueCondition = builder.makeCall(SqlStdOperatorTable.GREATER_THAN, new RexNode[]{builder.makeBigintLiteral(BigDecimal.ONE), builder.makeBigintLiteral(BigDecimal.ZERO)});
         JdbcJoin newJoin = new JdbcJoin(cluster, join.getTraitSet().replace(Rel.LOGICAL), left.getInput(), right.getInput(), trueCondition, join.getVariablesSet(), join.getJoinType(), pluginId);
         JdbcCrel fauxCrel = new JdbcCrel(cluster, newJoin.getTraitSet().replace(Rel.LOGICAL), newJoin, pluginId);
         LogicalFilter logicalFilter = LogicalFilter.create(fauxCrel, join.getCondition());
         return !JdbcFilterRule.matches(logicalFilter, fauxCrel) ? null : JdbcFilterRule.convert((Filter)logicalFilter, fauxCrel, pluginId);
      }
   }
}
