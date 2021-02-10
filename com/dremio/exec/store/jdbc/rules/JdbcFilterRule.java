package com.dremio.exec.store.jdbc.rules;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.MoreRelOptUtil.ContainsRexVisitor;
import com.dremio.exec.planner.logical.FilterRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.jdbc.legacy.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.rel.JdbcFilter;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcFilterRule extends JdbcUnaryConverterRule {
   private static final Logger logger = LoggerFactory.getLogger(JdbcFilterRule.class);
   public static final JdbcFilterRule CALCITE_INSTANCE = new JdbcFilterRule(LogicalFilter.class, "JdbcFilterRuleCrel");
   public static final JdbcFilterRule LOGICAL_INSTANCE = new JdbcFilterRule(FilterRel.class, "JdbcFilterRuleDrel");

   private JdbcFilterRule(Class<? extends Filter> clazz, String name) {
      super(clazz, name);
   }

   public RelNode convert(RelNode rel, JdbcCrel crel, StoragePluginId pluginId) {
      return convert((Filter)rel, crel, pluginId);
   }

   public static RelNode convert(Filter filter, JdbcCrel crel, StoragePluginId pluginId) {
      return new JdbcFilter(filter.getCluster(), filter.getTraitSet().replace(Rel.LOGICAL), crel.getInput(), filter.getCondition(), filter.getVariablesSet(), pluginId);
   }

   public boolean matches(RelOptRuleCall call) {
      return matches((Filter)call.rel(0), (JdbcCrel)call.rel(1));
   }

   public static boolean matches(Filter filter, JdbcCrel crel) {
      try {
         if (UnpushableTypeVisitor.hasUnpushableTypes(filter, (RexNode)filter.getCondition())) {
            logger.debug("Filter has types that are not pushable. Aborting pushdown.");
            return false;
         } else if (ContainsRexVisitor.hasContains(filter.getCondition())) {
            return false;
         } else {
            StoragePluginId pluginId = crel.getPluginId();
            if (pluginId == null) {
               return true;
            } else {
               JdbcConverterRule.RuleContext ruleContext = JdbcConverterRule.getRuleContext(pluginId, crel.getCluster().getRexBuilder());
               JdbcDremioSqlDialect dialect = getDialect(pluginId);
               boolean hasNoBitSupport = !dialect.supportsLiteral(CompleteType.BIT);
               Iterator var6 = filter.getChildExps().iterator();

               RexNode node;
               do {
                  if (!var6.hasNext()) {
                     return true;
                  }

                  node = (RexNode)var6.next();
                  if (!(Boolean)ruleContext.getSupportedExpressions().get(node) || !(Boolean)ruleContext.getSubqueryHasSamePluginId().get(node)) {
                     return false;
                  }
               } while(!hasNoBitSupport || !dialect.hasBooleanLiteralOrRexCallReturnsBoolean(node, true));

               logger.debug("Boolean literal used in filter when dialect doesn't support booleans. Aborting pushdown.");
               return false;
            }
         }
      } catch (ExecutionException var8) {
         throw new IllegalStateException("Failure while trying to evaluate pushdown.", var8);
      }
   }
}
