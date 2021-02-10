package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcSchemaFetcher;
import com.dremio.exec.store.jdbc.JdbcSchemaFetcherFactory;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin;
import com.dremio.exec.store.jdbc.JdbcPluginConfig.Builder;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class JdbcConf<T extends DialectConf<T, JdbcStoragePlugin>> extends DialectConf<T, JdbcStoragePlugin> {
   private static final Logger logger = LoggerFactory.getLogger(JdbcConf.class);
   protected static final String ENABLE_EXTERNAL_QUERY_LABEL = "Grant External Query access (Warning: External Query allows users with the Can Query privilege on this source to query any table or view within the source)";

   public abstract JdbcPluginConfig buildPluginConfig(Builder var1, CredentialsService var2, OptionManager var3);

   public JdbcStoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
      JdbcSchemaFetcherFactory factory = JdbcSchemaFetcherFactory.of(context.getConfig(), context.getJdbcSchemaFetcherFactoryContext());
      logger.debug("Plugin {} is using fetcher factory: {}", name, factory.getClass());
      JdbcSchemaFetcher schemaFetcher = factory.newFetcher(name, this);
      return new JdbcStoragePlugin(schemaFetcher.getConfig(), schemaFetcher, context.getConfig(), pluginIdProvider, context.getOptionManager().getOption(PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT));
   }
}
