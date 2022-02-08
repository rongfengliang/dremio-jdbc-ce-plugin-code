package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;

abstract class JdbcUnaryConverterRule extends JdbcConverterRule {
   JdbcUnaryConverterRule(Class<? extends RelNode> clazz, String description) {
      super(RelOptHelper.some(clazz, RelOptHelper.any(JdbcCrel.class), new RelOptRuleOperand[0]), description);
   }

   protected abstract RelNode convert(RelNode var1, JdbcCrel var2, StoragePluginId var3);

   public void onMatch(RelOptRuleCall call) {
      RelNode rel = call.rel(0);
      JdbcCrel crel = (JdbcCrel)call.rel(1);
      RelNode converted = this.convert(rel, crel, crel.getPluginId());
      if (converted != null) {
         JdbcCrel newCrel = new JdbcCrel(rel.getCluster(), converted.getTraitSet().replace(Rel.LOGICAL), converted, crel.getPluginId());
         call.transformTo(newCrel);
      }

   }
}
