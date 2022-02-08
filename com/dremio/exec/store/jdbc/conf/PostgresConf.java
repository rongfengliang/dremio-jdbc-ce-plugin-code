package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.catalog.conf.AuthenticationType;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.EncryptionValidationMode;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcPluginConfig.Builder;
import com.dremio.exec.store.jdbc.dialect.PostgreSQLDialect;
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
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

@SourceType(
   value = "POSTGRES",
   label = "PostgreSQL",
   uiConfig = "postgres-layout.json",
   externalQuerySupported = true
)
public class PostgresConf extends AbstractArpConf<PostgresConf> {
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
   @JsonIgnore
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
   @JsonIgnore
   public boolean enableExternalQuery;
   @Tag(13)
   public List<Property> propertyList;
   @Tag(14)
   @DisplayMetadata(
      label = "Maximum idle connections"
   )
   @NotMetadataImpacting
   public int maxIdleConns;
   @Tag(15)
   @DisplayMetadata(
      label = "Connection idle time (s)"
   )
   @NotMetadataImpacting
   public int idleTimeSec;
   @Tag(16)
   @DisplayMetadata(
      label = "Query timeout (s)"
   )
   @NotMetadataImpacting
   public int queryTimeoutSec;

   public PostgresConf() {
      this.encryptionValidationMode = EncryptionValidationMode.CERTIFICATE_AND_HOSTNAME_VALIDATION;
      this.enableExternalQuery = false;
      this.maxIdleConns = 8;
      this.idleTimeSec = 60;
      this.queryTimeoutSec = 0;
   }

   public JdbcPluginConfig buildPluginConfig(Builder configBuilder, CredentialsService credentialsService, OptionManager optionManager) {
      return configBuilder.withDialect(this.getDialect()).withDatasourceFactory(() -> {
         return this.newDataSource(credentialsService);
      }).withShowOnlyConnDatabase(false).withFetchSize(this.fetchSize).withQueryTimeout(this.queryTimeoutSec).build();
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
      if (null != this.propertyList) {
         this.propertyList.forEach((p) -> {
            properties.put(p.name, p.value);
         });
      }

      return DataSources.newGenericConnectionPoolDataSource("org.postgresql.Driver", this.toJdbcConnectionString(), this.username, credsFromCredentialsService != null ? credsFromCredentialsService.getPassword() : this.password, properties, DataSources.CommitMode.FORCE_MANUAL_COMMIT_MODE, this.maxIdleConns, (long)this.idleTimeSec);
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

   public PostgreSQLDialect getDialect() {
      return PG_ARP_DIALECT;
   }

   @VisibleForTesting
   public static PostgreSQLDialect getDialectSingleton() {
      return PG_ARP_DIALECT;
   }
}
