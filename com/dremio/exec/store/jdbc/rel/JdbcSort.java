package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.JdbcRelImpl;
import java.util.List;
import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

public class JdbcSort extends Sort implements JdbcRelImpl {
   private final StoragePluginId pluginId;

   public JdbcSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch, StoragePluginId pluginId) {
      super(cluster, traitSet, input, collation, offset, fetch);

      assert this.getConvention() == input.getConvention();

      this.pluginId = pluginId;
   }

   public JdbcSort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch) {
      return new JdbcSort(this.getCluster(), traitSet, newInput, newCollation, offset, fetch, this.pluginId);
   }

   public static boolean isCollationEmpty(Sort sort) {
      return sort.getCollation() == null || sort.getCollation() == RelCollations.EMPTY;
   }

   public static boolean isOffsetEmpty(Sort sort) {
      return sort.offset == null || (Long)((RexLiteral)sort.offset).getValue2() == 0L;
   }

   public static boolean isFetchEmpty(Sort sort) {
      return sort.fetch == null;
   }

   public RelNode copyWith(CopyWithCluster copier) {
      RelNode input = this.getInput().accept(copier);
      return new JdbcSort(copier.getCluster(), this.getTraitSet(), input, this.getCollation(), copier.copyOf(this.offset), copier.copyOf(this.fetch), this.pluginId);
   }

   public StoragePluginId getPluginId() {
      return this.pluginId;
   }

   public RelNode revert(List<RelNode> revertedInputs, RelBuilder builder) {
      return builder.push((RelNode)revertedInputs.get(0)).sortLimit(this.collation, this.offset, this.fetch).build();
   }
}
