package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.jdbc.rel.JdbcMinus;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcMinusRule extends JdbcBinaryConverterRule {
   private static final Logger logger = LoggerFactory.getLogger(JdbcMinusRule.class);
   public static final JdbcMinusRule INSTANCE = new JdbcMinusRule();

   private JdbcMinusRule() {
      super(LogicalMinus.class, "JdbcMinusRule");
   }

   public RelNode convert(RelNode rel, JdbcCrel left, JdbcCrel right, StoragePluginId pluginId) {
      LogicalMinus minus = (LogicalMinus)rel;
      if (minus.all) {
         logger.debug("EXCEPT All used but is not permitted to be pushed down. Aborting pushdown.");
         return null;
      } else {
         return new JdbcMinus(rel.getCluster(), rel.getTraitSet().replace(Rel.LOGICAL), ImmutableList.of(left.getInput(), right.getInput()), false, pluginId);
      }
   }
}
