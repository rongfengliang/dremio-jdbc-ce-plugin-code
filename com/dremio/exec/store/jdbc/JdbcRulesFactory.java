package com.dremio.exec.store.jdbc;

import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.store.StoragePluginRulesFactory.StoragePluginTypeRulesFactory;
import com.dremio.exec.store.jdbc.rules.JdbcAggregateRule;
import com.dremio.exec.store.jdbc.rules.JdbcCalcRule;
import com.dremio.exec.store.jdbc.rules.JdbcExpansionRule;
import com.dremio.exec.store.jdbc.rules.JdbcFilterRule;
import com.dremio.exec.store.jdbc.rules.JdbcFilterSetOpTransposeRule;
import com.dremio.exec.store.jdbc.rules.JdbcIntersectRule;
import com.dremio.exec.store.jdbc.rules.JdbcJoinRule;
import com.dremio.exec.store.jdbc.rules.JdbcLimitRule;
import com.dremio.exec.store.jdbc.rules.JdbcMinusRule;
import com.dremio.exec.store.jdbc.rules.JdbcProjectRule;
import com.dremio.exec.store.jdbc.rules.JdbcPrule;
import com.dremio.exec.store.jdbc.rules.JdbcSampleRule;
import com.dremio.exec.store.jdbc.rules.JdbcSortMergeRule;
import com.dremio.exec.store.jdbc.rules.JdbcSortRule;
import com.dremio.exec.store.jdbc.rules.JdbcTableModificationRule;
import com.dremio.exec.store.jdbc.rules.JdbcUnionRule;
import com.dremio.exec.store.jdbc.rules.JdbcValuesRule;
import com.dremio.exec.store.jdbc.rules.JdbcWindowRule;
import com.dremio.exec.store.jdbc.rules.scan.JdbcScanCrelRule;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;

public class JdbcRulesFactory extends StoragePluginTypeRulesFactory {
   public Set<RelOptRule> getRules(OptimizerRulesContext optimizerContext, PlannerPhase phase, SourceType pluginType) {
      switch(phase) {
      case LOGICAL:
         Builder<RelOptRule> logicalBuilder = ImmutableSet.builder();
         if (optimizerContext.getPlannerSettings().isRelPlanningEnabled()) {
            return logicalBuilder.add(new JdbcScanCrelRule(pluginType)).build();
         }

         return logicalBuilder.build();
      case JDBC_PUSHDOWN:
      case POST_SUBSTITUTION:
         Builder<RelOptRule> builder = ImmutableSet.builder();
         builder.add(new JdbcScanCrelRule(pluginType));
         builder.add(JdbcExpansionRule.INSTANCE);
         builder.add(JdbcAggregateRule.CALCITE_INSTANCE);
         builder.add(JdbcCalcRule.INSTANCE);
         builder.add(JdbcFilterRule.CALCITE_INSTANCE);
         builder.add(JdbcIntersectRule.INSTANCE);
         builder.add(JdbcJoinRule.CALCITE_INSTANCE);
         builder.add(JdbcMinusRule.INSTANCE);
         builder.add(JdbcProjectRule.CALCITE_INSTANCE);
         builder.add(JdbcSampleRule.CALCITE_INSTANCE);
         builder.add(JdbcSortRule.CALCITE_INSTANCE);
         builder.add(JdbcTableModificationRule.INSTANCE);
         builder.add(JdbcUnionRule.CALCITE_INSTANCE);
         builder.add(JdbcValuesRule.CALCITE_INSTANCE);
         builder.add(JdbcFilterSetOpTransposeRule.INSTANCE);
         return builder.build();
      case RELATIONAL_PLANNING:
         Builder<RelOptRule> jdbcBuilder = ImmutableSet.builder();
         jdbcBuilder.add(JdbcExpansionRule.INSTANCE);
         jdbcBuilder.add(JdbcAggregateRule.LOGICAL_INSTANCE);
         jdbcBuilder.add(JdbcFilterRule.LOGICAL_INSTANCE);
         jdbcBuilder.add(JdbcJoinRule.LOGICAL_INSTANCE);
         jdbcBuilder.add(JdbcProjectRule.LOGICAL_INSTANCE);
         jdbcBuilder.add(JdbcSampleRule.LOGICAL_INSTANCE);
         jdbcBuilder.add(JdbcSortRule.LOGICAL_INSTANCE);
         jdbcBuilder.add(JdbcUnionRule.LOGICAL_INSTANCE);
         jdbcBuilder.add(JdbcValuesRule.LOGICAL_INSTANCE);
         jdbcBuilder.add(JdbcFilterSetOpTransposeRule.INSTANCE);
         jdbcBuilder.add(JdbcLimitRule.INSTANCE);
         jdbcBuilder.add(JdbcSortMergeRule.INSTANCE);
         jdbcBuilder.add(JdbcWindowRule.INSTANCE);
         return jdbcBuilder.build();
      case PHYSICAL:
         Builder<RelOptRule> physicalBuilder = ImmutableSet.builder();
         physicalBuilder.add(new JdbcPrule(optimizerContext.getFunctionRegistry()));
         return physicalBuilder.build();
      default:
         return ImmutableSet.of();
      }
   }
}
