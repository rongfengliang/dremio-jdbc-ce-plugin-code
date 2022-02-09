package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.JdbcRelImpl;
import java.util.List;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.tools.RelBuilder;

public class JdbcTableModify extends TableModify implements JdbcRelImpl {
   private final StoragePluginId pluginId;

   public JdbcTableModify(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, CatalogReader catalogReader, RelNode input, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened, StoragePluginId pluginId) {
      super(cluster, traitSet, table, catalogReader, input, operation, updateColumnList, sourceExpressionList, flattened);
      this.pluginId = pluginId;
      ModifiableTable modifiableTable = (ModifiableTable)table.unwrap(ModifiableTable.class);
      if (modifiableTable == null) {
         throw new AssertionError();
      } else if (table.getExpression(Queryable.class) == null) {
         throw new AssertionError();
      }
   }

   public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
      return super.computeSelfCost(planner, mq).multiplyBy(0.1D);
   }

   public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new JdbcTableModify(this.getCluster(), traitSet, this.getTable(), this.getCatalogReader(), (RelNode)sole(inputs), this.getOperation(), this.getUpdateColumnList(), this.getSourceExpressionList(), this.isFlattened(), this.pluginId);
   }

   public RelNode copyWith(CopyWithCluster copier) {
      RelNode input = this.getInput().accept(copier);
      return new JdbcTableModify(copier.getCluster(), this.getTraitSet(), copier.copyOf(this.getTable()), this.getCatalogReader(), input, this.getOperation(), this.getUpdateColumnList(), copier.copyRexNodes(this.getSourceExpressionList()), this.isFlattened(), this.pluginId);
   }

   public StoragePluginId getPluginId() {
      return this.pluginId;
   }

   public LogicalTableModify revert(List<RelNode> revertedInputs, RelBuilder builder) {
      return LogicalTableModify.create(this.table, this.catalogReader, (RelNode)revertedInputs.get(0), this.getOperation(), this.getUpdateColumnList(), this.getSourceExpressionList(), this.isFlattened());
   }
}
