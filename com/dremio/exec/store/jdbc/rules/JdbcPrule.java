package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.common.JdbcRelImpl;
import com.dremio.exec.planner.logical.JdbcRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.store.jdbc.rel.JdbcIntermediatePrel;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

public final class JdbcPrule extends ConverterRule {
   private final FunctionLookupContext context;

   public JdbcPrule(FunctionLookupContext context) {
      super(JdbcRel.class, Rel.LOGICAL, Prel.PHYSICAL, "JDBC_PREL_Converter_");
      this.context = context;
   }

   public RelNode convert(RelNode in) {
      JdbcRel drel = (JdbcRel)in;
      RelTraitSet physicalTraits = drel.getTraitSet().replace(this.getOutTrait());
      physicalTraits = physicalTraits.replace(DistributionTrait.SINGLETON);
      return new JdbcIntermediatePrel(drel.getCluster(), physicalTraits, drel.getSubTree(), this.context, ((JdbcRelImpl)drel.getSubTree()).getPluginId());
   }
}
