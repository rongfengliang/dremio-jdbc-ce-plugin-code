package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.JdbcRelBase;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.planner.sql.handlers.PrelFinalizable;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

public class JdbcIntermediatePrel extends JdbcRelBase implements LeafPrel, PrelFinalizable {
   private final FunctionLookupContext context;
   private final StoragePluginId pluginId;

   public JdbcIntermediatePrel(RelOptCluster cluster, RelTraitSet traits, RelNode jdbcSubTree, FunctionLookupContext context, StoragePluginId pluginId) {
      super(cluster, traits, jdbcSubTree);
      this.context = context;
      this.pluginId = pluginId;
   }

   public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
      throw new UnsupportedOperationException();
   }

   public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new JdbcIntermediatePrel(this.getCluster(), traitSet, this.jdbcSubTree, this.context, this.pluginId);
   }

   public SelectionVectorMode getEncoding() {
      return SelectionVectorMode.NONE;
   }

   public Prel finalizeRel() {
      List<RexNode> projects = new ArrayList(this.getCluster().getRexBuilder().identityProjects(this.getRowType()));
      JdbcPrel input = new JdbcPrel(this.getCluster(), this.getTraitSet(), this, this.context, this.pluginId);
      return ProjectPrel.create(this.getCluster(), this.getTraitSet(), input, projects, this.rowType);
   }

   public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
      throw new UnsupportedOperationException("This needs to be finalized before using a PrelVisitor.");
   }

   public Iterator<Prel> iterator() {
      return Collections.emptyIterator();
   }

   public SelectionVectorMode[] getSupportedEncodings() {
      return SelectionVectorMode.DEFAULT;
   }

   public boolean needsFinalColumnReordering() {
      return true;
   }

   public int getMaxParallelizationWidth() {
      return 1;
   }

   public int getMinParallelizationWidth() {
      return 1;
   }

   public DistributionAffinity getDistributionAffinity() {
      return DistributionAffinity.SOFT;
   }
}
