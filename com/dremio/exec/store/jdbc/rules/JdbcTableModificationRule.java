package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.jdbc.rel.JdbcTableModify;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.schema.ModifiableTable;

public final class JdbcTableModificationRule extends JdbcUnaryConverterRule {
   public static final JdbcTableModificationRule INSTANCE = new JdbcTableModificationRule();

   private JdbcTableModificationRule() {
      super(LogicalTableModify.class, "JdbcTableModificationRule");
   }

   public RelNode convert(RelNode rel, JdbcCrel crel, StoragePluginId pluginId) {
      LogicalTableModify modify = (LogicalTableModify)rel;
      ModifiableTable modifiableTable = (ModifiableTable)modify.getTable().unwrap(ModifiableTable.class);
      return modifiableTable == null ? null : new JdbcTableModify(modify.getCluster(), modify.getTraitSet().replace(Rel.LOGICAL), modify.getTable(), modify.getCatalogReader(), crel.getInput(), modify.getOperation(), modify.getUpdateColumnList(), modify.getSourceExpressionList(), modify.isFlattened(), pluginId);
   }
}
