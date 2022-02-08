package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.logical.LimitRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.jdbc.rel.JdbcSort;
import java.util.List;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcLimitRule extends JdbcUnaryConverterRule {
   private static final Logger logger = LoggerFactory.getLogger(JdbcLimitRule.class);
   public static final JdbcLimitRule INSTANCE = new JdbcLimitRule();

   private JdbcLimitRule() {
      super(LimitRel.class, "JdbcLimitRuleDrel");
   }

   public boolean matches(RelOptRuleCall call) {
      LimitRel limit = (LimitRel)call.rel(0);
      JdbcCrel crel = (JdbcCrel)call.rel(1);
      StoragePluginId pluginId = crel.getPluginId();
      if (pluginId == null) {
         return true;
      } else {
         logger.debug("Checking if Limit node {} is supported using supportsSort().", limit);
         List<RelCollation> collationList = limit.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE);
         RelCollation collation = collationList.size() == 0 ? RelCollations.EMPTY : (RelCollation)collationList.get(0);
         boolean isSortSupported = getDialect(pluginId).supportsSort(LogicalSort.create(crel, collation, limit.getOffset(), limit.getFetch()));
         if (!isSortSupported) {
            logger.debug("Limit '{}' is unsupported. Aborting pushdown.", limit);
            return false;
         } else {
            logger.debug("Limit '{}' is supported.", limit);
            return true;
         }
      }
   }

   public RelNode convert(RelNode rel, JdbcCrel crel, StoragePluginId pluginId) {
      LimitRel limit = (LimitRel)rel;
      return new JdbcSort(rel.getCluster(), limit.getTraitSet().replace(Rel.LOGICAL).replace(RelCollations.EMPTY), crel.getInput(), RelCollations.EMPTY, limit.getOffset(), limit.getFetch(), pluginId);
   }
}
