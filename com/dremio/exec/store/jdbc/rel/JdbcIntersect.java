package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.JdbcRelImpl;
import com.dremio.exec.planner.logical.Rel;
import java.util.List;
import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.tools.RelBuilder;

public class JdbcIntersect extends Intersect implements JdbcRelImpl {
   private final StoragePluginId pluginId;

   public JdbcIntersect(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all, StoragePluginId pluginId) {
      super(cluster, traitSet, inputs, all);

      assert !all;

      this.pluginId = pluginId;
   }

   public JdbcIntersect copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new JdbcIntersect(this.getCluster(), traitSet, inputs, all, this.pluginId);
   }

   public RelNode copyWith(CopyWithCluster copier) {
      return new JdbcIntersect(copier.getCluster(), this.getTraitSet(), copier.visitAll(this.getInputs()), this.all, this.pluginId);
   }

   public StoragePluginId getPluginId() {
      return this.pluginId;
   }

   public LogicalIntersect revert(List<RelNode> revertedInputs, RelBuilder builder) {
      if (((RelNode)revertedInputs.get(0)).getTraitSet().contains(Rel.LOGICAL)) {
         throw new UnsupportedOperationException("Reverting JdbcIntersect with logical convention is not supported");
      } else {
         return LogicalIntersect.create(revertedInputs, this.all);
      }
   }
}
