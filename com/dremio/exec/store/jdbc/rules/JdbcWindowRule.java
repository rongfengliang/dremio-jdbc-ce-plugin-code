package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.WindowRel;
import com.dremio.exec.store.jdbc.legacy.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.rel.JdbcWindow;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcWindowRule extends JdbcUnaryConverterRule {
   private static final Logger logger = LoggerFactory.getLogger(JdbcWindowRule.class);
   public static final JdbcWindowRule INSTANCE = new JdbcWindowRule(WindowRel.class, "JdbcWindowRuleDrel");

   private JdbcWindowRule(Class<? extends Window> clazz, String name) {
      super(clazz, name);
   }

   public RelNode convert(RelNode rel, JdbcCrel crel, StoragePluginId pluginId) {
      Window window = (Window)rel;
      return new JdbcWindow(rel.getCluster(), rel.getTraitSet().replace(Rel.LOGICAL), crel.getInput(), window.getConstants(), window.getRowType(), window.groups, pluginId);
   }

   public boolean matches(RelOptRuleCall call) {
      Window window = (Window)call.rel(0);
      JdbcCrel jdbcCrel = (JdbcCrel)call.rel(1);
      StoragePluginId pluginId = jdbcCrel.getPluginId();
      if (pluginId == null) {
         return true;
      } else {
         JdbcDremioSqlDialect dialect = getDialect(pluginId);
         if (!dialect.supportsOver(window)) {
            logger.debug("Encountered unsupported OVER clause in window function. Aborting pushdown.");
            return false;
         } else {
            return true;
         }
      }
   }
}
