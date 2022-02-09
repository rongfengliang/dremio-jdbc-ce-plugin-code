package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.calcite.logical.SampleCrel;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.SampleRelBase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.SampleRel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.store.jdbc.rel.JdbcSort;
import java.math.BigDecimal;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;

public final class JdbcSampleRule extends JdbcUnaryConverterRule {
   private static final long SAMPLE_SIZE_DENOMINATOR = 5L;
   public static final JdbcSampleRule CALCITE_INSTANCE = new JdbcSampleRule(SampleCrel.class, "JdbcSampleRuleCrel");
   public static final JdbcSampleRule LOGICAL_INSTANCE = new JdbcSampleRule(SampleRel.class, "JdbcSampleRuleDrel");

   private JdbcSampleRule(Class<? extends SampleRelBase> clazz, String name) {
      super(clazz, name);
   }

   public boolean matches(RelOptRuleCall call) {
      JdbcCrel crel = (JdbcCrel)call.rel(1);
      StoragePluginId pluginId = crel.getPluginId();
      return pluginId == null ? true : getDialect(pluginId).supportsSort(true, true);
   }

   public RelNode convert(RelNode rel, JdbcCrel crel, StoragePluginId pluginId) {
      SampleRelBase sample = (SampleRelBase)rel;
      PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(sample.getCluster().getPlanner());
      RexBuilder rexBuilder = sample.getCluster().getRexBuilder();
      return new JdbcSort(rel.getCluster(), sample.getTraitSet().replace(Rel.LOGICAL), crel.getInput(), RelCollations.EMPTY, rexBuilder.makeBigintLiteral(BigDecimal.ZERO), rexBuilder.makeBigintLiteral(BigDecimal.valueOf(SampleCrel.getSampleSizeAndSetMinSampleSize(plannerSettings, 5L))), pluginId);
   }
}
