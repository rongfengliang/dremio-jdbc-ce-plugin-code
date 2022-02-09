package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.SortRel;
import com.dremio.exec.store.jdbc.rel.JdbcSort;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcSortRule extends JdbcUnaryConverterRule {
   private static final Logger logger = LoggerFactory.getLogger(JdbcSortRule.class);
   public static final JdbcSortRule CALCITE_INSTANCE = new JdbcSortRule(LogicalSort.class, "JdbcSortRuleCrel");
   public static final JdbcSortRule LOGICAL_INSTANCE = new JdbcSortRule(SortRel.class, "JdbcSortRuleDrel");

   private JdbcSortRule(Class<? extends Sort> clazz, String name) {
      super(clazz, name);
   }

   public boolean matches(RelOptRuleCall call) {
      Sort sort = (Sort)call.rel(0);
      JdbcCrel crel = (JdbcCrel)call.rel(1);
      StoragePluginId pluginId = crel.getPluginId();
      if (pluginId == null) {
         return true;
      } else {
         logger.debug("Checking if Sort node {} is supported using supportsSort().", sort);
         boolean isSortSupported = getDialect(pluginId).supportsSort(sort);
         if (!isSortSupported) {
            logger.debug("Sort '{}' is unsupported. Aborting pushdown.", sort);
            return false;
         } else {
            logger.debug("Sort '{}' is supported.", sort);
            return true;
         }
      }
   }

   public RelNode convert(RelNode rel, JdbcCrel crel, StoragePluginId pluginId) {
      Sort sort = (Sort)rel;
      return new JdbcSort(rel.getCluster(), sort.getTraitSet().replace(Rel.LOGICAL), crel.getInput(), sort.getCollation(), sort.offset, sort.fetch, pluginId);
   }
}
