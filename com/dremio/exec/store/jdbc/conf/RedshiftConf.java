package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.catalog.conf.AuthenticationType;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcPluginConfig.Builder;
import com.dremio.exec.store.jdbc.dialect.RedshiftDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.legacy.LegacyCapableJdbcConf;
import com.dremio.exec.store.jdbc.legacy.LegacyDialect;
import com.dremio.exec.store.jdbc.legacy.RedshiftLegacyDialect;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import com.dremio.security.PasswordCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.protostuff.Tag;
import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.Properties;

@SourceType(
   value = "REDSHIFT",
   label = "Amazon Redshift",
   uiConfig = "redshift-layout.json"
)
public class RedshiftConf extends LegacyCapableJdbcConf<RedshiftConf> {
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
   public boolean useLegacyDialect = false;
   @Tag(9)
   @DisplayMetadata(
      label = "Secret resource url"
   )
   public String secretResourceUrl;
   @Tag(10)
   @NotMetadataImpacting
   @DisplayMetadata(
      label = "Grant External Query access (Warning: External Query allows users with the Can Query privilege on this source to query any table or view within the source)"
   )
   public boolean enableExternalQuery = false;

   public JdbcPluginConfig buildPluginConfig(Builder configBuilder, CredentialsService credentialsService, OptionManager optionManager) {
      return configBuilder.withDialect(this.getDialect()).withDatasourceFactory(() -> {
         return this.newDataSource(credentialsService);
      }).withShowOnlyConnDatabase(false).withFetchSize(this.fetchSize).withAllowExternalQuery(this.supportsExternalQuery(this.enableExternalQuery)).build();
   }

   private CloseableDataSource newDataSource(CredentialsService credentialsService) throws SQLException {
      PasswordCredentials credsFromCredentialsService = null;
      if (!Strings.isNullOrEmpty(this.secretResourceUrl)) {
         try {
            URI secretURI = URI.create(this.secretResourceUrl);
            credsFromCredentialsService = (PasswordCredentials)credentialsService.getCredentials(secretURI);
         } catch (IOException var4) {
            throw new SQLException(var4.getMessage(), var4);
         }
      }

      return DataSources.newGenericConnectionPoolDataSource("com.amazon.redshift.jdbc.Driver", (String)Preconditions.checkNotNull(this.connectionString, "missing connection URL"), this.username, credsFromCredentialsService != null ? credsFromCredentialsService.getPassword() : this.password, (Properties)null, DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE);
   }

   protected LegacyDialect getLegacyDialect() {
      return RedshiftLegacyDialect.INSTANCE;
   }

   protected ArpDialect getArpDialect() {
      return REDSHIFT_ARP_DIALECT;
   }

   protected boolean getLegacyFlag() {
      return this.useLegacyDialect;
   }

   public static RedshiftConf newMessage() {
      RedshiftConf result = new RedshiftConf();
      result.useLegacyDialect = true;
      return result;
   }

   @VisibleForTesting
   public static RedshiftDialect getDialectSingleton() {
      return REDSHIFT_ARP_DIALECT;
   }
}
