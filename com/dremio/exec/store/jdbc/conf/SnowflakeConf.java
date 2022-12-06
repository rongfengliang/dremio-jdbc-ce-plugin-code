package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcPluginConfig.Builder;
import com.dremio.exec.store.jdbc.dialect.SnowflakeDialect;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.protostuff.Tag;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import javax.validation.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SourceType(
   value = "SNOWFLAKE",
   label = "Snowflake",
   uiConfig = "snowflake-layout.json",
   externalQuerySupported = true,
   previewEngineRequired = true
)
public class SnowflakeConf extends AbstractArpConf<SnowflakeConf> {
   private static final Logger logger = LoggerFactory.getLogger(SnowflakeConf.class);
   private static final String DRIVER_CLASS_NAME = "net.snowflake.client.jdbc.SnowflakeDriver";
   private static final String ARP_FILENAME = "arp/implementation/snowflake-arp.yaml";
   private static final SnowflakeDialect SNOWFLAKE_ARP_DIALECT = (SnowflakeDialect)AbstractArpConf.loadArpFile("arp/implementation/snowflake-arp.yaml", SnowflakeDialect::new);
   private static final Boolean ALLOW_EXTERNAL_QUERY = true;
   private static final int CONNECTION_PORT = 443;
   @NotBlank
   @Tag(1)
   @DisplayMetadata(
      label = "Host"
   )
   public String hostname;
   @Tag(2)
   @DisplayMetadata(
      label = "Database"
   )
   public String database;
   @Tag(3)
   @DisplayMetadata(
      label = "Role"
   )
   public String role;
   @Tag(4)
   @DisplayMetadata(
      label = "Schema"
   )
   public String schema;
   @Tag(5)
   @DisplayMetadata(
      label = "Warehouse"
   )
   public String warehouse;
   @NotBlank
   @Tag(6)
   @DisplayMetadata(
      label = "Username"
   )
   public String username;
   @NotBlank
   @Tag(7)
   @DisplayMetadata(
      label = "Password"
   )
   @Secret
   public String password;
   @Tag(8)
   @DisplayMetadata(
      label = "Maximum idle connections"
   )
   @NotMetadataImpacting
   public int maxIdleConns = 8;
   @Tag(9)
   @DisplayMetadata(
      label = "Connection idle time (s)"
   )
   @NotMetadataImpacting
   public int idleTimeSec = 60;
   @Tag(10)
   @DisplayMetadata(
      label = "Query timeout (s)"
   )
   @NotMetadataImpacting
   public int queryTimeoutSec = 0;
   @Tag(11)
   public List<Property> propertyList;

   @VisibleForTesting
   public JdbcPluginConfig buildPluginConfig(Builder configBuilder, CredentialsService credentialsService, OptionManager optionManager) {
      configBuilder.withDialect(this.getDialect()).withDatasourceFactory(this::newDataSource).withQueryTimeout(this.queryTimeoutSec).withAllowExternalQuery(ALLOW_EXTERNAL_QUERY);
      if (!Strings.isNullOrEmpty(this.database)) {
         configBuilder.withDatabase(this.database).withShowOnlyConnDatabase(true);
      }

      return configBuilder.build();
   }

   private CloseableDataSource newDataSource() throws SQLException {
      Preconditions.checkNotNull(this.hostname, "missing hostname");
      Preconditions.checkNotNull(this.username, "missing username");
      Preconditions.checkNotNull(this.password, "missing password");
      String url = String.format("jdbc:snowflake://%s:%d/?db=%s&schema=%s&warehouse=%s", this.hostname, 443, this.database, this.schema, this.warehouse);
      if (!Strings.isNullOrEmpty(this.role)) {
         url = url + "&role=" + this.role;
      }

      Properties connectionParameters = new Properties();
      if (null != this.propertyList) {
         this.propertyList.forEach((p) -> {
            connectionParameters.put(p.name, p.value);
         });
      }

      return DataSources.newGenericConnectionPoolDataSource("net.snowflake.client.jdbc.SnowflakeDriver", url, this.username, this.password, connectionParameters, DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE, this.maxIdleConns, (long)this.idleTimeSec);
   }

   public SnowflakeDialect getDialect() {
      return SNOWFLAKE_ARP_DIALECT;
   }

   @VisibleForTesting
   public static SnowflakeDialect getDialectSingleton() {
      return SNOWFLAKE_ARP_DIALECT;
   }
}
