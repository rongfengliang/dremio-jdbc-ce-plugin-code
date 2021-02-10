package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.JdbcRelImpl;
import java.util.List;
import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilder;

public class JdbcUnion extends Union implements JdbcRelImpl {
   private final StoragePluginId pluginId;

   public JdbcUnion(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all, StoragePluginId pluginId) {
      super(cluster, traitSet, inputs, all);
      this.pluginId = pluginId;
   }

   public JdbcUnion copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new JdbcUnion(this.getCluster(), traitSet, inputs, all, this.pluginId);
   }

   public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
      return super.computeSelfCost(planner, mq).multiplyBy(0.1D);
   }

   public StoragePluginId getPluginId() {
      return this.pluginId;
   }

   public RelNode revert(List<RelNode> revertedInputs, RelBuilder builder) {
      return builder.pushAll(revertedInputs).union(this.all).build();
   }

   public RelNode copyWith(CopyWithCluster copier) {
      return new JdbcUnion(copier.getCluster(), this.getTraitSet(), copier.visitAll(this.getInputs()), this.all, this.pluginId);
   }
}
