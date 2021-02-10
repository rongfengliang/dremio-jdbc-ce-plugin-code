package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.JdbcRelImpl;
import java.util.List;
import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.tools.RelBuilder;

public class JdbcMinus extends Minus implements JdbcRelImpl {
   private final StoragePluginId pluginId;

   public JdbcMinus(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all, StoragePluginId pluginId) {
      super(cluster, traitSet, inputs, all);

      assert !all;

      this.pluginId = pluginId;
   }

   public JdbcMinus copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new JdbcMinus(this.getCluster(), traitSet, inputs, all, this.pluginId);
   }

   public RelNode copyWith(CopyWithCluster copier) {
      return new JdbcMinus(copier.getCluster(), this.getTraitSet(), copier.visitAll(this.getInputs()), this.all, this.pluginId);
   }

   public StoragePluginId getPluginId() {
      return this.pluginId;
   }

   public LogicalMinus revert(List<RelNode> revertedInputs, RelBuilder builder) {
      return LogicalMinus.create(revertedInputs, this.all);
   }
}
