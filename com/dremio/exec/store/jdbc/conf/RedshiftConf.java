package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.catalog.conf.AuthenticationType;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcPluginConfig.Builder;
import com.dremio.exec.store.jdbc.dialect.RedshiftDialect;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import com.dremio.security.PasswordCredentials;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.protostuff.Tag;
import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

@SourceType(
   value = "REDSHIFT",
   label = "Amazon Redshift",
   uiConfig = "redshift-layout.json",
   externalQuerySupported = true
)
public class RedshiftConf extends AbstractArpConf<RedshiftConf> {
   private static final String ARP_FILENAME = "arp/implementation/redshift-arp.yaml";
   private static final RedshiftDialect REDSHIFT_ARP_DIALECT = (RedshiftDialect)AbstractArpConf.loadArpFile("arp/implementation/redshift-arp.yaml", RedshiftDialect::new);
   private static final String DRIVER = "com.amazon.redshift.jdbc.Driver";
   @Tag(1)
   @DisplayMetadata(
      label = "JDBC Connection String"
   )
   public String connectionString;
   @Tag(4)
   public String username;
   @Tag(5)
   @Secret
   public String password;
   @Tag(6)
   public AuthenticationType authenticationType;
   @Tag(7)
   @DisplayMetadata(
      label = "Record fetch size"
   )
   @NotMetadataImpacting
   public int fetchSize = 200;
   @Tag(8)
   @DisplayMetadata(
      label = "Enable legacy dialect"
   )
   @JsonIgnore
   public boolean useLegacyDialect = false;
   @Tag(9)
   @DisplayMetadata(
      label = "Secret resource url"
   )
   public String secretResourceUrl;
   @Tag(10)
   @NotMetadataImpacting
   @JsonIgnore
   public boolean enableExternalQuery = false;
   @Tag(11)
   public List<Property> propertyList;
   @Tag(12)
   @DisplayMetadata(
      label = "Maximum idle connections"
   )
   @NotMetadataImpacting
   public int maxIdleConns = 8;
   @Tag(13)
   @DisplayMetadata(
      label = "Connection idle time (s)"
   )
   @NotMetadataImpacting
   public int idleTimeSec = 60;
   @Tag(14)
   @DisplayMetadata(
      label = "AWS Profile"
   )
   public String awsProfile;
   @Tag(15)
   @DisplayMetadata(
      label = "DbUser"
   )
   public String dbUser;
   @Tag(16)
   @DisplayMetadata(
      label = "Query timeout (s)"
   )
   @NotMetadataImpacting
   public int queryTimeoutSec = 0;

   public JdbcPluginConfig buildPluginConfig(Builder configBuilder, CredentialsService credentialsService, OptionManager optionManager) {
      return configBuilder.withDialect(this.getDialect()).withDatasourceFactory(() -> {
         return this.newDataSource(credentialsService);
      }).withShowOnlyConnDatabase(false).withFetchSize(this.fetchSize).withQueryTimeout(this.queryTimeoutSec).build();
   }

   private CloseableDataSource newDataSource(CredentialsService credentialsService) throws SQLException {
      Preconditions.checkNotNull(this.connectionString, "missing connection URL");
      String overrideConnectionString = null;
      if (this.authenticationType == AuthenticationType.AWS_PROFILE) {
         String prefixToReplace = "jdbc:redshift://";
         String desiredPrefix = "jdbc:redshift:iam://";
         if (this.connectionString.startsWith("jdbc:redshift://")) {
            overrideConnectionString = "jdbc:redshift:iam://" + this.connectionString.substring("jdbc:redshift://".length());
         }
      }

      PasswordCredentials credsFromCredentialsService = null;
      if (!Strings.isNullOrEmpty(this.secretResourceUrl)) {
         try {
            URI secretURI = URI.create(this.secretResourceUrl);
            credsFromCredentialsService = (PasswordCredentials)credentialsService.getCredentials(secretURI);
         } catch (IOException var5) {
            throw new SQLException(var5.getMessage(), var5);
         }
      }

      Properties props = new Properties();
      if (null != this.propertyList) {
         this.propertyList.forEach((p) -> {
            props.put(p.name, p.value);
         });
      }

      if (!Strings.isNullOrEmpty(this.awsProfile)) {
         props.put("profile", this.awsProfile);
      }

      if (!Strings.isNullOrEmpty(this.dbUser)) {
         props.put("DbUser", this.dbUser);
      }

      return DataSources.newGenericConnectionPoolDataSource("com.amazon.redshift.jdbc.Driver", overrideConnectionString != null ? overrideConnectionString : this.connectionString, this.username, credsFromCredentialsService != null ? credsFromCredentialsService.getPassword() : this.password, props, DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE, this.maxIdleConns, (long)this.idleTimeSec);
   }

   public RedshiftDialect getDialect() {
      return REDSHIFT_ARP_DIALECT;
   }

   @VisibleForTesting
   public static RedshiftDialect getDialectSingleton() {
      return REDSHIFT_ARP_DIALECT;
   }
}
