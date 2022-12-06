package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.sql.handlers.RexSubQueryUtils.RexSubQueryPushdownChecker;
import com.dremio.exec.store.jdbc.conf.DialectConf;
import com.dremio.exec.store.jdbc.dialect.JdbcDremioSqlDialect;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;

class JdbcRuleContext {
   private static final Map<StoragePluginId, JdbcRuleContext> ruleContextCache;
   private final StoragePluginId pluginId;
   private final RexBuilder builder;
   private final LoadingCache<RexNode, Boolean> subqueryHasSamePluginId;
   private final LoadingCache<RexNode, Boolean> overCheckedExpressions;
   private final LoadingCache<RexNode, Boolean> supportedExpressions;

   JdbcRuleContext(StoragePluginId pluginId, RexBuilder builder) {
      this.subqueryHasSamePluginId = CacheBuilder.newBuilder().maximumSize(1000L).expireAfterWrite(10L, TimeUnit.MINUTES).build(new CacheLoader<RexNode, Boolean>() {
         public Boolean load(RexNode expr) {
            return expr instanceof RexSubQuery ? (new RexSubQueryPushdownChecker(JdbcRuleContext.this.getPluginId())).canPushdownRexSubQuery() : true;
         }
      });
      this.overCheckedExpressions = CacheBuilder.newBuilder().maximumSize(1000L).expireAfterWrite(10L, TimeUnit.MINUTES).build(new CacheLoader<RexNode, Boolean>() {
         public Boolean load(RexNode expr) {
            return JdbcOverCheck.hasOver(expr, JdbcRuleContext.this.getPluginId());
         }
      });
      this.supportedExpressions = CacheBuilder.newBuilder().maximumSize(1000L).expireAfterWrite(10L, TimeUnit.MINUTES).build(new CacheLoader<RexNode, Boolean>() {
         public Boolean load(RexNode expr) {
            return JdbcExpressionSupportCheck.hasOnlySupportedFunctions(expr, JdbcRuleContext.this.getPluginId(), JdbcRuleContext.this.builder);
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

   protected static JdbcDremioSqlDialect getDialect(StoragePluginId pluginId) {
      DialectConf<?, ?> conf = (DialectConf)pluginId.getConnectionConf();
      return conf.getDialect();
   }

   static JdbcRuleContext getRuleContext(StoragePluginId pluginId, RexBuilder builder) {
      JdbcRuleContext context = (JdbcRuleContext)ruleContextCache.get(pluginId);
      if (context == null) {
         context = new JdbcRuleContext(pluginId, builder);
         ruleContextCache.put(pluginId, context);
      }

      return context;
   }

   static {
      ruleContextCache = CacheBuilder.newBuilder().maximumSize(100L).expireAfterWrite(10L, TimeUnit.MINUTES).weakKeys().weakValues().build().asMap();
   }
}
