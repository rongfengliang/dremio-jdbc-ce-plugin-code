package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.store.jdbc.rel.JdbcSort;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcSortMergeRule extends RelOptRule {
   private static final Logger logger = LoggerFactory.getLogger(JdbcSortMergeRule.class);
   public static final JdbcSortMergeRule INSTANCE = new JdbcSortMergeRule();

   private JdbcSortMergeRule() {
      super(operand(JdbcSort.class, operand(JdbcSort.class, any()), new RelOptRuleOperand[0]), "JdbcSortMergeRule");
   }

   public boolean matches(RelOptRuleCall call) {
      return true;
   }

   public void onMatch(RelOptRuleCall call) {
      JdbcSort sort1 = (JdbcSort)call.rel(0);
      JdbcSort sort2 = (JdbcSort)call.rel(1);
      JdbcSort sorted = sort1.getCollation() != RelCollations.EMPTY ? sort1 : sort2;
      RexNode offset = sort1.offset == null ? sort2.offset : sort1.offset;
      RexNode fetch = sort1.fetch == null ? sort2.fetch : sort1.fetch;
      StoragePluginId pluginId = sort1.getPluginId();
      JdbcSort mergedSort = new JdbcSort(sort1.getCluster(), sorted.getTraitSet(), sort2.getInput(), sorted.getCollation(), offset, fetch, pluginId);
      call.transformTo(mergedSort);
   }
}
