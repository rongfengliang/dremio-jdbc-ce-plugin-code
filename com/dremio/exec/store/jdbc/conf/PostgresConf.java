package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.catalog.conf.AuthenticationType;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.EncryptionValidationMode;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcPluginConfig.Builder;
import com.dremio.exec.store.jdbc.dialect.PostgreSQLDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.legacy.LegacyCapableJdbcConf;
import com.dremio.exec.store.jdbc.legacy.LegacyDialect;
import com.dremio.exec.store.jdbc.legacy.PostgreSQLLegacyDialect;
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
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

@SourceType(
   value = "POSTGRES",
   label = "PostgreSQL",
   uiConfig = "postgres-layout.json"
)
public class PostgresConf extends LegacyCapableJdbcConf<PostgresConf> {
   private static final String ARP_FILENAME = "arp/implementation/postgresql-arp.yaml";
   private static final PostgreSQLDialect PG_ARP_DIALECT = (PostgreSQLDialect)AbstractArpConf.loadArpFile("arp/implementation/postgresql-arp.yaml", PostgreSQLDialect::new);
   private static final String DRIVER = "org.postgresql.Driver";
   @NotBlank
   @Tag(1)
   @DisplayMetadata(
      label = "Host"
   )
   public String hostname;
   @NotBlank
   @Tag(2)
   @Min(1L)
   @Max(65535L)
   @DisplayMetadata(
      label = "Port"
   )
   public String port = "5432";
   @NotBlank
   @Tag(3)
   @DisplayMetadata(
      label = "Database Name"
   )
   public String databaseName;
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
      label = "Encrypt connection"
   )
   @NotMetadataImpacting
   public boolean useSsl = false;
   @Tag(10)
   @NotMetadataImpacting
   @DisplayMetadata(
      label = "Validation Mode"
   )
   public EncryptionValidationMode encryptionValidationMode;
   @Tag(11)
   @DisplayMetadata(
      label = "Secret resource url"
   )
   public String secretResourceUrl;
   @Tag(12)
   @NotMetadataImpacting
   @DisplayMetadata(
      label = "Grant External Query access (Warning: External Query allows users with the Can Query privilege on this source to query any table or view within the source)"
   )
   public boolean enableExternalQuery;

   public PostgresConf() {
      this.encryptionValidationMode = EncryptionValidationMode.CERTIFICATE_AND_HOSTNAME_VALIDATION;
      this.enableExternalQuery = false;
   }

   public JdbcPluginConfig buildPluginConfig(Builder configBuilder, CredentialsService credentialsService, OptionManager optionManager) {
      return configBuilder.withDialect(this.getDialect()).withDatasourceFactory(() -> {
         return this.newDataSource(credentialsService);
      }).withShowOnlyConnDatabase(false).withFetchSize(this.fetchSize).withAllowExternalQuery(this.supportsExternalQuery(this.enableExternalQuery)).build();
   }

   private CloseableDataSource newDataSource(CredentialsService credentialsService) throws SQLException {
      Properties properties = new Properties();
      PasswordCredentials credsFromCredentialsService = null;
      if (!Strings.isNullOrEmpty(this.secretResourceUrl)) {
         try {
            URI secretURI = URI.create(this.secretResourceUrl);
            credsFromCredentialsService = (PasswordCredentials)credentialsService.getCredentials(secretURI);
         } catch (IOException var5) {
            throw new SQLException(var5.getMessage(), var5);
         }
      }

      if (this.useSsl) {
         properties.setProperty("ssl", "true");
         properties.setProperty("sslmode", this.getSslMode());
      }

      properties.setProperty("OpenSourceSubProtocolOverride", "true");
      return DataSources.newGenericConnectionPoolDataSource("org.postgresql.Driver", this.toJdbcConnectionString(), this.username, credsFromCredentialsService != null ? credsFromCredentialsService.getPassword() : this.password, properties, DataSources.CommitMode.FORCE_MANUAL_COMMIT_MODE);
   }

   private String toJdbcConnectionString() {
      String hostname = (String)Preconditions.checkNotNull(this.hostname, "missing hostname");
      String portAsString = (String)Preconditions.checkNotNull(this.port, "missing port");
      int port = Integer.parseInt(portAsString);
      String db = (String)Preconditions.checkNotNull(this.databaseName, "missing database");
      return String.format("jdbc:postgresql://%s:%d/%s", hostname, port, db);
   }

   private String getSslMode() {
      Preconditions.checkNotNull(this.encryptionValidationMode, "missing validation mode");
      switch(this.encryptionValidationMode) {
      case CERTIFICATE_AND_HOSTNAME_VALIDATION:
         return "verify-full";
      case CERTIFICATE_ONLY_VALIDATION:
         return "verify-ca";
      case NO_VALIDATION:
         return "require";
      default:
         throw new IllegalStateException(this.encryptionValidationMode + " is unknown");
      }
   }

   protected LegacyDialect getLegacyDialect() {
      return PostgreSQLLegacyDialect.INSTANCE;
   }

   protected ArpDialect getArpDialect() {
      return PG_ARP_DIALECT;
   }

   protected boolean getLegacyFlag() {
      return this.useLegacyDialect;
   }

   public static PostgresConf newMessage() {
      PostgresConf result = new PostgresConf();
      result.useLegacyDialect = true;
      return result;
   }

   @VisibleForTesting
   public static PostgreSQLDialect getDialectSingleton() {
      return PG_ARP_DIALECT;
   }
}
