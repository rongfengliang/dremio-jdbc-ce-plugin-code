package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.planner.logical.DremioRelFactories;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;

public final class JdbcFilterSetOpTransposeRule extends FilterSetOpTransposeRule {
   public static final JdbcFilterSetOpTransposeRule INSTANCE = new JdbcFilterSetOpTransposeRule();

   private JdbcFilterSetOpTransposeRule() {
      super(DremioRelFactories.CALCITE_LOGICAL_BUILDER);
   }

   public boolean matches(RelOptRuleCall call) {
      Filter filterRel = (Filter)call.rel(0);
      SetOp setOp = (SetOp)call.rel(1);
      return filterRel.getConvention() == Convention.NONE && setOp.getConvention() == Convention.NONE;
   }
}
