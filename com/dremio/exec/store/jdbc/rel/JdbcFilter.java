package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.JdbcRelImpl;
import com.dremio.exec.planner.common.MoreRelOptUtil.ContainsRexVisitor;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

public class JdbcFilter extends Filter implements JdbcRelImpl {
   private final boolean foundContains;
   private final ImmutableSet<CorrelationId> variablesSet;
   private final StoragePluginId pluginId;

   public JdbcFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode condition, Set<CorrelationId> variablesSet, StoragePluginId pluginId) {
      super(cluster, traitSet, input, condition);
      this.pluginId = pluginId;
      this.foundContains = ContainsRexVisitor.hasContains(condition);
      this.variablesSet = ImmutableSet.copyOf(variablesSet);
   }

   public Set<CorrelationId> getVariablesSet() {
      return this.variablesSet;
   }

   public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
      return this.foundContains ? planner.getCostFactory().makeInfiniteCost() : super.computeSelfCost(planner, mq);
   }

   public JdbcFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
      return new JdbcFilter(this.getCluster(), traitSet, input, condition, this.variablesSet, this.pluginId);
   }

   public RelNode copyWith(CopyWithCluster copier) {
      RelNode input = this.getInput().accept(copier);
      return new JdbcFilter(copier.getCluster(), this.getTraitSet(), input, copier.copyOf(this.getCondition()), this.variablesSet, this.pluginId);
   }

   public StoragePluginId getPluginId() {
      return this.pluginId;
   }

   public Filter revert(List<RelNode> revertedInputs, RelBuilder builder) {
      return (Filter)builder.push((RelNode)revertedInputs.get(0)).filter(new RexNode[]{this.condition}).build();
   }
}
