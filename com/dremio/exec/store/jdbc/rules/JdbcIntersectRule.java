package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.jdbc.rel.JdbcIntersect;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcIntersectRule extends JdbcBinaryConverterRule {
   private static final Logger logger = LoggerFactory.getLogger(JdbcIntersectRule.class);
   public static final JdbcIntersectRule INSTANCE = new JdbcIntersectRule();

   private JdbcIntersectRule() {
      super(LogicalIntersect.class, "JdbcIntersectRule");
   }

   public RelNode convert(RelNode rel, JdbcCrel left, JdbcCrel right, StoragePluginId pluginId) {
      LogicalIntersect intersect = (LogicalIntersect)rel;
      if (intersect.all) {
         logger.debug("Intersect All used but is not permitted to be pushed down. Aborting pushdown.");
         return null;
      } else {
         return new JdbcIntersect(rel.getCluster(), intersect.getTraitSet().replace(Rel.LOGICAL), ImmutableList.of(left.getInput(), right.getInput()), false, pluginId);
      }
   }
}
