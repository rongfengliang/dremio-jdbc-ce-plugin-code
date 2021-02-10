package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.jdbc.rel.JdbcCalc;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rex.RexMultisetUtil;

public final class JdbcCalcRule extends JdbcUnaryConverterRule {
   public static final JdbcCalcRule INSTANCE = new JdbcCalcRule();

   private JdbcCalcRule() {
      super(LogicalCalc.class, "JdbcCalcRule");
   }

   public RelNode convert(RelNode rel, JdbcCrel crel, StoragePluginId pluginId) {
      LogicalCalc calc = (LogicalCalc)rel;
      return RexMultisetUtil.containsMultiset(calc.getProgram()) ? null : new JdbcCalc(rel.getCluster(), rel.getTraitSet().replace(Rel.LOGICAL), crel.getInput(), calc.getProgram(), pluginId);
   }
}
