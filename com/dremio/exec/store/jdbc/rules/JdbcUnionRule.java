package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.common.SampleRelBase;
import com.dremio.exec.planner.common.MoreRelOptUtil.SubsetRemover;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.UnionRel;
import com.dremio.exec.store.jdbc.legacy.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.rel.JdbcUnion;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcUnionRule extends JdbcBinaryConverterRule {
   private static final Logger logger = LoggerFactory.getLogger(JdbcUnionRule.class);
   public static final JdbcUnionRule CALCITE_INSTANCE = new JdbcUnionRule(LogicalUnion.class, "JdbcUnionRuleCrel");
   public static final JdbcUnionRule LOGICAL_INSTANCE = new JdbcUnionRule(UnionRel.class, "JdbcUnionRuleDrel");

   private JdbcUnionRule(Class<? extends Union> clazz, String name) {
      super(clazz, name);
   }

   public boolean matchImpl(RelOptRuleCall call) {
      Union union = (Union)call.rel(0);
      JdbcCrel leftChild = (JdbcCrel)call.rel(1);
      StoragePluginId pluginId = leftChild.getPluginId();
      if (pluginId == null) {
         return true;
      } else {
         JdbcDremioSqlDialect dialect = getDialect(pluginId);
         logger.debug("Checking if Union is supported on dialect using supportsUnion{}", union.all ? "All" : "");
         boolean supportsUnionClause = union.all && dialect.supportsUnionAll() || dialect.supportsUnion();
         if (!supportsUnionClause) {
            logger.debug("Union operation wasn't supported. Aborting pushdown.");
            return false;
         } else if (dialect.supportsFetchOffsetInSetOperand()) {
            logger.debug("Union operation supported. Pushing down Union.");
            return true;
         } else {
            logger.debug("Dialect does not support using Sort in set operands. Scanning for a Sort.");
            RelNode subtree = union.accept(new SubsetRemover());
            JdbcUnionRule.SortDetector sortDetector = new JdbcUnionRule.SortDetector((Union)subtree);
            subtree.accept(sortDetector);
            return !sortDetector.hasFoundSort();
         }
      }
   }

   public RelNode convert(RelNode rel, JdbcCrel left, JdbcCrel right, StoragePluginId pluginId) {
      Union union = (Union)rel;
      RelTraitSet traitSet = union.getTraitSet();
      return new JdbcUnion(rel.getCluster(), traitSet.replace(Rel.LOGICAL), ImmutableList.of(left.getInput(), right.getInput()), union.all, pluginId);
   }

   private static final class SortDetector extends StatelessRelShuttleImpl {
      private boolean hasSort = false;
      private final Union rootUnion;

      public SortDetector(Union union) {
         this.rootUnion = union;
      }

      protected RelNode visitChildren(RelNode node) {
         if (node == this.rootUnion) {
            return super.visitChildren(node);
         } else if (this.hasSort) {
            return node;
         } else if (node instanceof Sort) {
            Sort sortNode = (Sort)node;
            if (sortNode.fetch == null && sortNode.offset == null) {
               return super.visitChildren(node);
            } else {
               this.hasSort = true;
               return node;
            }
         } else if (node instanceof SampleRelBase) {
            this.hasSort = true;
            return node;
         } else {
            return !(node instanceof TableScan) && !(node instanceof SetOp) && !(node instanceof Join) && !(node instanceof com.dremio.common.logical.data.Join) ? super.visitChildren(node) : node;
         }
      }

      public boolean hasFoundSort() {
         return this.hasSort;
      }
   }
}
