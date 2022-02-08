package com.dremio.exec.store.jdbc;

import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.server.JdbcSchemaFetcherFactoryContext;
import com.dremio.exec.store.jdbc.JdbcPluginConfig.Builder;
import com.dremio.exec.store.jdbc.conf.JdbcConf;
import com.dremio.exec.store.jdbc.conf.JdbcConstants;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import com.google.common.primitives.Ints;
import java.util.function.Supplier;

public class JdbcSchemaFetcherFactoryImpl implements JdbcSchemaFetcherFactory {
   private final OptionManager optionManager;
   private final CredentialsService credentialsService;

   public JdbcSchemaFetcherFactoryImpl(JdbcSchemaFetcherFactoryContext context) {
      this.optionManager = context.getOptionManager();
      this.credentialsService = context.getCredentialsService();
   }

   public JdbcSchemaFetcher newFetcher(String sourceName, ConnectionConf<?, ?> connectionConf) {
      JdbcConf<?> jdbcConf = (JdbcConf)connectionConf;
      Supplier<Integer> rowCountQueryTimeout = () -> {
         return Ints.saturatedCast(this.optionManager.getOption(JdbcConstants.JDBC_ROW_COUNT_QUERY_TIMEOUT_VALIDATOR));
      };
      Builder configBuilder = JdbcPluginConfig.newBuilder().withSourceName(sourceName).withRowCountQueryTimeOut(rowCountQueryTimeout);
      JdbcPluginConfig pluginConfig = jdbcConf.buildPluginConfig(configBuilder, this.credentialsService, this.optionManager);
      return pluginConfig.getDialect().newSchemaFetcher(pluginConfig);
   }
}
