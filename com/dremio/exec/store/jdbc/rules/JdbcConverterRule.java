package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.sql.handlers.RexSubQueryUtils.RexSubQueryPushdownChecker;
import com.dremio.exec.store.jdbc.conf.DialectConf;
import com.dremio.exec.store.jdbc.dialect.JdbcDremioSqlDialect;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.tools.RelBuilderFactory;

abstract class JdbcConverterRule extends RelOptRule {
   private static final Map<WeakReference<StoragePluginId>, JdbcConverterRule.RuleContext> ruleContextCache = new ConcurrentHashMap();

   JdbcConverterRule(RelOptRuleOperand operand, String description) {
      super(operand, description);
   }

   JdbcConverterRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
      super(operand, relBuilderFactory, description);
   }

   protected static JdbcDremioSqlDialect getDialect(StoragePluginId pluginId) {
      DialectConf<?, ?> conf = (DialectConf)pluginId.getConnectionConf();
      return conf.getDialect();
   }

   static JdbcConverterRule.RuleContext getRuleContext(StoragePluginId pluginId, RexBuilder builder) {
      JdbcConverterRule.RuleContext context = (JdbcConverterRule.RuleContext)ruleContextCache.get(new WeakReference(pluginId));
      if (context == null) {
         context = new JdbcConverterRule.RuleContext(pluginId, builder);
         ruleContextCache.put(new WeakReference(pluginId), context);
      }

      return context;
   }

   static class RuleContext {
      private final StoragePluginId pluginId;
      private final RexBuilder builder;
      private final LoadingCache<RexNode, Boolean> subqueryHasSamePluginId;
      private final LoadingCache<RexNode, Boolean> overCheckedExpressions;
      private final LoadingCache<RexNode, Boolean> supportedExpressions;

      RuleContext(StoragePluginId pluginId, RexBuilder builder) {
         this.subqueryHasSamePluginId = CacheBuilder.newBuilder().maximumSize(1000L).expireAfterWrite(10L, TimeUnit.MINUTES).build(new CacheLoader<RexNode, Boolean>() {
            public Boolean load(RexNode expr) {
               return expr instanceof RexSubQuery ? (new RexSubQueryPushdownChecker(RuleContext.this.getPluginId())).canPushdownRexSubQuery() : true;
            }
         });
         this.overCheckedExpressions = CacheBuilder.newBuilder().maximumSize(1000L).expireAfterWrite(10L, TimeUnit.MINUTES).build(new CacheLoader<RexNode, Boolean>() {
            public Boolean load(RexNode expr) {
               return JdbcOverCheck.hasOver(expr, RuleContext.this.getPluginId());
            }
         });
         this.supportedExpressions = CacheBuilder.newBuilder().maximumSize(1000L).expireAfterWrite(10L, TimeUnit.MINUTES).build(new CacheLoader<RexNode, Boolean>() {
            public Boolean load(RexNode expr) {
               return JdbcExpressionSupportCheck.hasOnlySupportedFunctions(expr, RuleContext.this.getPluginId(), RuleContext.this.builder);
            }
         });
         this.pluginId = pluginId;
         this.builder = builder;
      }

      StoragePluginId getPluginId() {
         return this.pluginId;
      }

      LoadingCache<RexNode, Boolean> getSubqueryHasSamePluginId() {
         return this.subqueryHasSamePluginId;
      }

      LoadingCache<RexNode, Boolean> getOverCheckedExpressions() {
         return this.overCheckedExpressions;
      }

      LoadingCache<RexNode, Boolean> getSupportedExpressions() {
         return this.supportedExpressions;
      }
   }
}
