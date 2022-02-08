package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.JdbcRelImpl;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

public class JdbcJoin extends Join implements JdbcRelImpl {
   private final StoragePluginId pluginId;

   public JdbcJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType, StoragePluginId pluginId) {
      super(cluster, traitSet, left, right, condition, variablesSet, joinType);
      this.pluginId = pluginId;
   }

   public JdbcJoin copy(RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
      return new JdbcJoin(this.getCluster(), traitSet, left, right, condition, this.variablesSet, joinType, this.pluginId);
   }

   public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
      double rowCount = mq.getRowCount(this);
      return planner.getCostFactory().makeCost(rowCount, 0.0D, 0.0D);
   }

   public double estimateRowCount(RelMetadataQuery mq) {
      double leftRowCount = mq.getRowCount(this.left);
      double rightRowCount = mq.getRowCount(this.right);
      return Math.max(leftRowCount, rightRowCount);
   }

   public RelNode copyWith(CopyWithCluster copier) {
      RelNode left = this.getLeft().accept(copier);
      RelNode right = this.getRight().accept(copier);
      return new JdbcJoin(this.getCluster(), this.getTraitSet(), left, right, copier.copyOf(this.getCondition()), this.getVariablesSet(), this.getJoinType(), this.pluginId);
   }

   public StoragePluginId getPluginId() {
      return this.pluginId;
   }

   public Join revert(List<RelNode> revertedInputs, RelBuilder builder) {
      return (Join)builder.push((RelNode)revertedInputs.get(0)).push((RelNode)revertedInputs.get(1)).join(this.joinType, this.condition, this.variablesSet).build();
   }
}
