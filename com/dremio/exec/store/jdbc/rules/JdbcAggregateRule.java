package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.AggregateRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.jdbc.ColumnPropertyAccumulator;
import com.dremio.exec.store.jdbc.dialect.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.rel.JdbcAggregate;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcAggregateRule extends JdbcUnaryConverterRule {
   private static final Logger logger = LoggerFactory.getLogger(JdbcAggregateRule.class);
   public static final JdbcAggregateRule CALCITE_INSTANCE = new JdbcAggregateRule(LogicalAggregate.class, "JdbcAggregateRuleCrel");
   public static final JdbcAggregateRule LOGICAL_INSTANCE = new JdbcAggregateRule(AggregateRel.class, "JdbcAggregateRuleDrel");

   private JdbcAggregateRule(Class<? extends Aggregate> clazz, String name) {
      super(clazz, name);
   }

   public RelNode convert(RelNode rel, JdbcCrel crel, StoragePluginId pluginId) {
      Aggregate agg = (Aggregate)rel;
      if (agg.getGroupSets().size() != 1) {
         return null;
      } else {
         try {
            return new JdbcAggregate(rel.getCluster(), crel.getTraitSet().replace(Rel.LOGICAL), crel.getInput(), agg.getGroupSet(), agg.getGroupSets(), agg.getAggCallList(), pluginId);
         } catch (InvalidRelException var6) {
            logger.debug(var6.toString());
            return null;
         }
      }
   }

   public boolean matches(RelOptRuleCall call) {
      Aggregate aggregate = (Aggregate)call.rel(0);
      JdbcCrel crel = (JdbcCrel)call.rel(1);
      StoragePluginId pluginId = crel.getPluginId();
      if (pluginId == null) {
         return true;
      } else {
         JdbcDremioSqlDialect dialect = getDialect(pluginId);
         logger.debug("Checking if source RDBMS supports aggregation.");
         if (!dialect.supportsAggregation()) {
            logger.debug("Aggregations are unsupported.");
            return false;
         } else if (this.hasUnpushableTypes(aggregate)) {
            return false;
         } else {
            Iterator var6 = aggregate.getAggCallList().iterator();

            while(var6.hasNext()) {
               AggregateCall aggCall = (AggregateCall)var6.next();
               logger.debug("Aggregate expression: {}", aggCall);
               if (!dialect.supportsDistinct() && aggCall.isDistinct()) {
                  logger.debug("Distinct used and distinct is not supported by dialect. Aborting pushdown.");
                  return false;
               }

               if (aggCall.getAggregation().getKind() == SqlKind.COUNT) {
                  logger.debug("Evaluating count support.");
                  boolean supportsCount = dialect.supportsCount(aggCall);
                  if (!supportsCount) {
                     logger.debug("Count operation unsupported. Aborting pushdown.");
                     return false;
                  }

                  logger.debug("Count operation supported.");
               } else {
                  if (!dialect.supportsAggregateFunction(aggCall.getAggregation().getKind())) {
                     logger.debug("Aggregate function {} not supported by dialect. Aborting pushdown.", aggCall.getAggregation().getName());
                     return false;
                  }

                  List<RelDataType> types = SqlTypeUtil.projectTypes(aggregate.getInput().getRowType(), aggCall.getArgList());
                  logger.debug("Checking if aggregate function {} used with types {} is supported by dialect using supportsFunction.", aggCall.getAggregation().getName(), types);
                  if (!dialect.supportsFunction(aggCall, types)) {
                     logger.debug("Aggregate {} with type {} not supported by dialect. Aborting pushdown", aggCall.getAggregation().getName(), types);
                     return false;
                  }
               }
            }

            if (!dialect.supportsBooleanAggregation()) {
               logger.debug("This dialect does not support boolean aggregations. Verifying no aggregate calls are on booleans.");
               List<RelDataTypeField> inputRowType = aggregate.getInput().getRowType().getFieldList();
               Iterator var12 = aggregate.getAggCallList().iterator();

               while(var12.hasNext()) {
                  AggregateCall aggregateCall = (AggregateCall)var12.next();
                  Iterator var9 = aggregateCall.getArgList().iterator();

                  while(var9.hasNext()) {
                     int argIndex = (Integer)var9.next();
                     if (((RelDataTypeField)inputRowType.get(argIndex)).getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
                        return false;
                     }
                  }
               }
            }

            return true;
         }
      }
   }

   private boolean hasUnpushableTypes(Aggregate aggregate) {
      Set<Integer> projectedIndexes = new HashSet(aggregate.getGroupSet().asSet());
      Iterator var3 = aggregate.getAggCallList().iterator();

      while(var3.hasNext()) {
         AggregateCall aggCall = (AggregateCall)var3.next();
         projectedIndexes.addAll(aggCall.getArgList());
      }

      ColumnPropertyAccumulator accumulator = new ColumnPropertyAccumulator();
      aggregate.accept(accumulator);
      RelNode input = aggregate.getInput();

      while(input.getRowType() == null) {
         if (input instanceof HepRelVertex) {
            input = ((HepRelVertex)input).getCurrentRel();
         } else if (input instanceof JdbcCrel) {
            input = ((JdbcCrel)input).getInput();
         } else if (input instanceof SingleRel) {
            input = ((SingleRel)input).getInput();
         }
      }

      Iterator var5 = projectedIndexes.iterator();

      Integer index;
      do {
         if (!var5.hasNext()) {
            if (UnpushableTypeVisitor.hasUnpushableTypes(input, MoreRelOptUtil.getChildExps(input))) {
               logger.debug("Aggregate has types that are not pushable. Aborting pushdown.");
               return true;
            }

            return false;
         }

         index = (Integer)var5.next();
      } while(!UnpushableTypeVisitor.hasUnpushableType((String)input.getRowType().getFieldNames().get(index), accumulator.getColumnProperties()));

      logger.debug("Aggregate has types that are not pushable. Aborting pushdown.");
      return true;
   }
}
