package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.JdbcRelImpl;
import com.dremio.exec.planner.logical.Rel;
import java.util.List;
import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;

public class JdbcCalc extends SingleRel implements JdbcRelImpl {
   private final RexProgram program;
   private final StoragePluginId pluginId;

   public JdbcCalc(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexProgram program, StoragePluginId pluginId) {
      super(cluster, traitSet, input);
      this.program = program;
      this.rowType = program.getOutputRowType();
      this.pluginId = pluginId;
   }

   /** @deprecated */
   @Deprecated
   public JdbcCalc(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexProgram program, int flags, StoragePluginId pluginId) {
      this(cluster, traitSet, input, program, pluginId);
      Util.discard(flags);
   }

   public RelWriter explainTerms(RelWriter pw) {
      return this.program.explainCalc(super.explainTerms(pw));
   }

   public double estimateRowCount(RelMetadataQuery mq) {
      return RelMdUtil.estimateFilteredRows(this.getInput(), this.program, mq);
   }

   public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
      double dRows = mq.getRowCount(this);
      double dCpu = mq.getRowCount(this.getInput()) * (double)this.program.getExprCount();
      double dIo = 0.0D;
      return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
   }

   public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new JdbcCalc(this.getCluster(), traitSet, (RelNode)sole(inputs), this.program, this.pluginId);
   }

   public RelNode copyWith(CopyWithCluster copier) {
      RelNode input = this.getInput().accept(copier);
      return new JdbcCalc(copier.getCluster(), this.getTraitSet(), input, copier.copyOf(this.program), this.pluginId);
   }

   public StoragePluginId getPluginId() {
      return this.pluginId;
   }

   public LogicalCalc revert(List<RelNode> revertedInputs, RelBuilder builder) {
      if (((RelNode)revertedInputs.get(0)).getTraitSet().contains(Rel.LOGICAL)) {
         throw new UnsupportedOperationException("Reverting JdbcCalc with logical convention is not supported");
      } else {
         return LogicalCalc.create((RelNode)revertedInputs.get(0), this.program);
      }
   }
}
