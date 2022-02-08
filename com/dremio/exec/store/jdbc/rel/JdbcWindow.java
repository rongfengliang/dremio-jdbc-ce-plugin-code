package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.JdbcRelImpl;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.common.WindowRelBase;
import com.dremio.exec.planner.logical.WindowRel;
import java.util.List;
import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window.Group;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilder;

public class JdbcWindow extends WindowRelBase implements JdbcRelImpl {
   private final StoragePluginId pluginId;

   public JdbcWindow(RelOptCluster cluster, RelTraitSet traits, RelNode child, List<RexLiteral> constants, RelDataType rowType, List<Group> windows, StoragePluginId pluginId) {
      super(cluster, traits, child, constants, MoreRelOptUtil.uniqifyFieldName(rowType, cluster.getTypeFactory()), windows);
      this.pluginId = pluginId;
   }

   public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new JdbcWindow(this.getCluster(), traitSet, (RelNode)sole(inputs), this.constants, this.getRowType(), this.groups, this.pluginId);
   }

   public RelNode copyWith(CopyWithCluster copier) {
      RelNode input = this.getInput().accept(copier);
      return new JdbcWindow(copier.getCluster(), this.getTraitSet(), input, this.constants, copier.copyOf(this.getRowType()), this.groups, this.pluginId);
   }

   public StoragePluginId getPluginId() {
      return this.pluginId;
   }

   public RelNode revert(List<RelNode> revertedInputs, RelBuilder builder) {
      RelNode input = (RelNode)revertedInputs.get(0);
      return WindowRel.create(input.getCluster(), input.getTraitSet(), input, this.constants, this.rowType, this.groups);
   }
}
