package com.dremio.exec.store.jdbc.rules;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.logical.ProjectRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.jdbc.legacy.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.rel.JdbcProject;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcProjectRule extends JdbcUnaryConverterRule {
   private static final Logger logger = LoggerFactory.getLogger(JdbcProjectRule.class);
   public static final JdbcProjectRule CALCITE_INSTANCE = new JdbcProjectRule(LogicalProject.class, "JdbcProjectRuleCrel");
   public static final JdbcProjectRule LOGICAL_INSTANCE = new JdbcProjectRule(ProjectRel.class, "JdbcProjectRuleDrel");

   private JdbcProjectRule(Class<? extends Project> clazz, String name) {
      super(clazz, name);
   }

   public RelNode convert(RelNode rel, JdbcCrel crel, StoragePluginId pluginId) {
      Project project = (Project)rel;
      return new JdbcProject(rel.getCluster(), rel.getTraitSet().replace(Rel.LOGICAL), crel.getInput(), project.getProjects(), project.getRowType(), pluginId);
   }

   public boolean matches(RelOptRuleCall call) {
      try {
         Project project = (Project)call.rel(0);
         JdbcCrel crel = (JdbcCrel)call.rel(1);
         StoragePluginId pluginId = crel.getPluginId();
         if (pluginId == null) {
            return true;
         } else if (UnpushableTypeVisitor.hasUnpushableTypes(project, (List)project.getChildExps())) {
            logger.debug("Project has expressions with types that are not pushable. Aborting pushdown.");
            return false;
         } else {
            JdbcConverterRule.RuleContext ruleContext = JdbcConverterRule.getRuleContext(pluginId, crel.getCluster().getRexBuilder());
            JdbcDremioSqlDialect dialect = getDialect(pluginId);
            boolean supportsBitLiteral = dialect.supportsLiteral(CompleteType.BIT);
            Iterator var8 = project.getChildExps().iterator();

            RexNode node;
            do {
               if (!var8.hasNext()) {
                  var8 = project.getChildExps().iterator();

                  do {
                     if (!var8.hasNext()) {
                        return true;
                     }

                     node = (RexNode)var8.next();
                  } while((Boolean)ruleContext.getOverCheckedExpressions().get(node));

                  logger.debug("Encountered unsupported OVER clause in window function. Aborting pushdown.");
                  return false;
               }

               node = (RexNode)var8.next();
               if (!(Boolean)ruleContext.getSupportedExpressions().get(node)) {
                  return false;
               }

               if (!supportsBitLiteral && dialect.hasBooleanLiteralOrRexCallReturnsBoolean(node, false)) {
                  logger.debug("Dialect does not support booleans, and an expression which returned a boolean was projected.Aborting pushdown.");
                  return false;
               }
            } while(!SqlTypeName.DAY_INTERVAL_TYPES.contains(node.getType().getSqlTypeName()) && !SqlTypeName.YEAR_INTERVAL_TYPES.contains(node.getType().getSqlTypeName()));

            logger.debug("Intervals are currently unsupported for projection by the JDBC plugin.");
            return false;
         }
      } catch (ExecutionException var10) {
         throw new IllegalStateException("Failure while trying to evaluate pushdown.", var10);
      }
   }
}
