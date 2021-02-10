package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.JdbcRelImpl;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.ValuesRel;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.tools.RelBuilder;

public class JdbcValues extends Values implements JdbcRelImpl {
   public JdbcValues(RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traitSet) {
      super(cluster, rowType, tuples, traitSet);
   }

   public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert inputs.isEmpty();

      return new JdbcValues(this.getCluster(), this.rowType, this.tuples, traitSet);
   }

   public RelNode copyWith(CopyWithCluster copier) {
      return new JdbcValues(copier.getCluster(), copier.copyOf(this.getRowType()), copier.copyOf(this.getTuples()), this.getTraitSet());
   }

   public StoragePluginId getPluginId() {
      return null;
   }

   public Values revert(List<RelNode> revertedInputs, RelBuilder builder) {
      assert revertedInputs.isEmpty();

      LogicalValues values = LogicalValues.create(this.getCluster(), this.rowType, this.tuples);
      return (Values)(this.getTraitSet().contains(Rel.LOGICAL) ? ValuesRel.from(values) : values);
   }
}
