package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.ValuesRel;
import com.dremio.exec.store.jdbc.rel.JdbcValues;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalValues;

public final class JdbcValuesRule extends ConverterRule {
   public static final JdbcValuesRule CALCITE_INSTANCE;
   public static final JdbcValuesRule LOGICAL_INSTANCE;

   private JdbcValuesRule(Class<? extends Values> clazz, RelTrait in, String name) {
      super(clazz, in, Rel.LOGICAL, name);
   }

   public RelNode convert(RelNode rel) {
      Values values = (Values)rel;
      JdbcValues jdbcValues = new JdbcValues(values.getCluster(), values.getRowType(), values.getTuples(), values.getTraitSet().replace(Rel.LOGICAL));
      return new JdbcCrel(values.getCluster(), values.getTraitSet().replace(Rel.LOGICAL), jdbcValues, jdbcValues.getPluginId());
   }

   static {
      CALCITE_INSTANCE = new JdbcValuesRule(LogicalValues.class, Convention.NONE, "JdbcValuesRuleCrel");
      LOGICAL_INSTANCE = new JdbcValuesRule(ValuesRel.class, Rel.LOGICAL, "JdbcValuesRuleDrel");
   }
}
