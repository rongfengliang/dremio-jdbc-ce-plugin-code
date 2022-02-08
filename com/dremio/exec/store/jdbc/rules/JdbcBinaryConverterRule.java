package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.logical.Rel;
import java.util.Optional;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilderFactory;

abstract class JdbcBinaryConverterRule extends JdbcConverterRule {
   JdbcBinaryConverterRule(Class<? extends RelNode> clazz, String description) {
      super(operand(clazz, operand(JdbcCrel.class, any()), new RelOptRuleOperand[]{operand(JdbcCrel.class, any())}), description);
   }

   JdbcBinaryConverterRule(Class<? extends RelNode> clazz, RelBuilderFactory relBuilderFactory, String description) {
      super(operand(clazz, operand(JdbcCrel.class, any()), new RelOptRuleOperand[]{operand(JdbcCrel.class, any())}), relBuilderFactory, description);
   }

   protected abstract RelNode convert(RelNode var1, JdbcCrel var2, JdbcCrel var3, StoragePluginId var4);

   public void onMatch(RelOptRuleCall call) {
      RelNode rel = call.rel(0);
      JdbcCrel leftCrel = (JdbcCrel)call.rel(1);
      JdbcCrel rightCrel = (JdbcCrel)call.rel(2);
      StoragePluginId pluginId = (StoragePluginId)Optional.ofNullable(leftCrel.getPluginId()).orElse(rightCrel.getPluginId());
      RelNode converted = this.convert(rel, leftCrel, rightCrel, pluginId);
      if (converted != null) {
         JdbcCrel crel = new JdbcCrel(rel.getCluster(), converted.getTraitSet().replace(Rel.LOGICAL), converted, pluginId);
         call.transformTo(crel);
      }

   }

   public final boolean matches(RelOptRuleCall call) {
      if (call.rel(0).getInputs().size() != 2) {
         return false;
      } else {
         JdbcCrel leftCrel = (JdbcCrel)call.rel(1);
         JdbcCrel rightCrel = (JdbcCrel)call.rel(2);
         if (leftCrel.getPluginId() != null && rightCrel.getPluginId() != null) {
            return !leftCrel.getPluginId().equals(rightCrel.getPluginId()) ? false : this.matchImpl(call);
         } else {
            return true;
         }
      }
   }

   protected boolean matchImpl(RelOptRuleCall call) {
      return true;
   }
}
