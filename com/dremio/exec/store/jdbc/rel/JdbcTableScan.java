package com.dremio.exec.store.jdbc.rel;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.JdbcRelImpl;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.TableMetadata;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelBuilder;

public class JdbcTableScan extends ScanRelBase implements JdbcRelImpl, Rel {
   private final List<String> fullPathMinusPluginName;
   private final boolean directNamespaceDescendent;

   public JdbcTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId, TableMetadata tableMetadata, List<SchemaPath> projectedColumns, double observedRowcountAdjustment, boolean directNamespaceDescendent) {
      super(cluster, traitSet, table, pluginId, tableMetadata, projectedColumns, observedRowcountAdjustment);
      this.fullPathMinusPluginName = ImmutableList.copyOf(table.getQualifiedName().listIterator(1));
      this.directNamespaceDescendent = directNamespaceDescendent;
   }

   public ScanRelBase cloneWithProject(List<SchemaPath> projection) {
      return new JdbcTableScan(this.getCluster(), this.traitSet, this.table, this.pluginId, this.tableMetadata, projection, this.observedRowcountAdjustment, this.directNamespaceDescendent);
   }

   public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      assert inputs.isEmpty();

      return new JdbcTableScan(this.getCluster(), traitSet, this.table, this.pluginId, this.tableMetadata, this.getProjectedColumns(), this.observedRowcountAdjustment, this.directNamespaceDescendent);
   }

   public boolean isDirectNamespaceDescendent() {
      return this.directNamespaceDescendent;
   }

   public SqlIdentifier getTableName() {
      return new SqlIdentifier(this.fullPathMinusPluginName, SqlParserPos.ZERO);
   }

   public RelNode copyWith(CopyWithCluster copier) {
      return new JdbcTableScan(this.getCluster(), copier.copyOf(this.traitSet), copier.copyOf(this.getTable()), this.pluginId, this.tableMetadata, this.getProjectedColumns(), this.observedRowcountAdjustment, this.directNamespaceDescendent);
   }

   public RelNode revert(List<RelNode> revertedInputs, RelBuilder builder) {
      assert revertedInputs.isEmpty();

      throw new AssertionError("JdbcTableScan should not be reverted");
   }
}
