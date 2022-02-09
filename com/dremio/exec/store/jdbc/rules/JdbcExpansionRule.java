package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.logical.RelOptHelper;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;

public final class JdbcExpansionRule extends JdbcConverterRule {
   public static final RelOptRule INSTANCE = new JdbcExpansionRule();

   private JdbcExpansionRule() {
      super(RelOptHelper.some(ExpansionNode.class, RelOptHelper.any(JdbcCrel.class), new RelOptRuleOperand[0]), "jdbc-expansion-removal");
   }

   public void onMatch(RelOptRuleCall call) {
      call.transformTo(call.rel(1));
   }
}
