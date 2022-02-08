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
import com.dremio.exec.store.jdbc.dialect.OracleDialect;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import com.dremio.security.PasswordCredentials;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import io.protostuff.Tag;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import javax.security.auth.x500.X500Principal;
import javax.sql.ConnectionPoolDataSource;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import org.apache.commons.lang3.reflect.MethodUtils;

@SourceType(
   value = "ORACLE",
   label = "Oracle",
   uiConfig = "oracle-layout.json",
   externalQuerySupported = true
)
public class OracleConf extends AbstractArpConf<OracleConf> {
   private static final String ARP_FILENAME = "arp/implementation/oracle-arp.yaml";
   private static final OracleDialect ORACLE_ARP_DIALECT = (OracleDialect)AbstractArpConf.loadArpFile("arp/implementation/oracle-arp.yaml", OracleDialect::new);
   private static final String POOLED_DATASOURCE = "oracle.jdbc.pool.OracleConnectionPoolDataSource";
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
   public String port = "1521";
   @NotBlank
   @Tag(3)
   @DisplayMetadata(
      label = "Service Name"
   )
   public String instance;
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
      label = "Enable TLS encryption"
   )
   @NotMetadataImpacting
   public boolean useSsl = false;
   @Tag(9)
   @DisplayMetadata(
      label = "SSL/TLS server certificate distinguished name"
   )
   @NotMetadataImpacting
   public String sslServerCertDN;
   @Tag(10)
   @DisplayMetadata(
      label = "Use timezone as connection region"
   )
   @NotMetadataImpacting
   public boolean useTimezoneAsRegion = true;
   @Tag(11)
   @DisplayMetadata(
      label = "Enable legacy dialect"
   )
   @JsonIgnore
   public boolean useLegacyDialect = false;
   @Tag(12)
   @DisplayMetadata(
      label = "Include synonyms"
   )
   public boolean includeSynonyms = false;
   @Tag(13)
   @DisplayMetadata(
      label = "Secret resource url"
   )
   public String secretResourceUrl;
   @Tag(14)
   @NotMetadataImpacting
   @JsonIgnore
   public boolean enableExternalQuery = false;
   @Tag(15)
   @DisplayMetadata(
      label = "Use LDAP Naming Services"
   )
   public boolean useLdap = false;
   @Tag(16)
   @DisplayMetadata(
      label = "Set DN for LDAP Naming Services"
   )
   public String bindDN;
   @Tag(17)
   @DisplayMetadata(
      label = "Oracle Native Encryption"
   )
   public OracleConf.OracleNativeEncryption nativeEncryption;
   @Tag(18)
   @DisplayMetadata(
      label = "Use Kerberos"
   )
   public boolean useKerberos;
   @Tag(19)
   public List<Property> propertyList;
   @Tag(20)
   @DisplayMetadata(
      label = "Maximum idle connections"
   )
   @NotMetadataImpacting
   public int maxIdleConns;
   @Tag(21)
   @DisplayMetadata(
      label = "Connection idle time (s)"
   )
   @NotMetadataImpacting
   public int idleTimeSec;
   @Tag(22)
   @DisplayMetadata(
      label = "Map Oracle DATE columns to TIMESTAMP"
   )
   public boolean mapDateToTimestamp;
   @Tag(23)
   @DisplayMetadata(
      label = "Query timeout (s)"
   )
   @NotMetadataImpacting
   public int queryTimeoutSec;

   public OracleConf() {
      this.nativeEncryption = OracleConf.OracleNativeEncryption.ACCEPTED;
      this.maxIdleConns = 8;
      this.idleTimeSec = 60;
      this.mapDateToTimestamp = true;
      this.queryTimeoutSec = 0;
   }

   public JdbcPluginConfig buildPluginConfig(Builder configBuilder, CredentialsService credentialsService, OptionManager optionManager) {
      configBuilder.withDialect(this.getDialect()).withShowOnlyConnDatabase(false).withFetchSize(this.fetchSize).addGenericProperty("MAP_DATE_TO_TIMESTAMP", Boolean.toString(this.mapDateToTimestamp)).withQueryTimeout(this.queryTimeoutSec).withDatasourceFactory(() -> {
         return this.newDataSource(credentialsService);
      });
      if (!this.includeSynonyms) {
         configBuilder.addHiddenTableType("SYNONYM", new String[0]);
      }

      return configBuilder.build();
   }

   private CloseableDataSource newDataSource(CredentialsService credentialsService) throws SQLException {
      if (this.authenticationType == AuthenticationType.MASTER) {
         Preconditions.checkNotNull(this.username, "missing username");
         Preconditions.checkNotNull(this.password, "missing password");
      }

      Preconditions.checkNotNull(this.hostname, "missing hostname");
      Preconditions.checkNotNull(this.port, "missing port");
      Preconditions.checkNotNull(this.instance, "missing instance");

      ConnectionPoolDataSource source;
      try {
         source = (ConnectionPoolDataSource)Class.forName("oracle.jdbc.pool.OracleConnectionPoolDataSource").newInstance();
      } catch (ReflectiveOperationException var4) {
         throw new RuntimeException("Cannot instantiate Oracle datasource", var4);
      }

      return this.newDataSource(source, credentialsService);
   }

   @VisibleForTesting
   CloseableDataSource newDataSource(ConnectionPoolDataSource dataSource, CredentialsService credentialsService) throws SQLException {
      Properties properties = new Properties();
      int portAsInteger = Integer.parseInt(this.port);
      properties.put("oracle.jdbc.timezoneAsRegion", Boolean.toString(this.useTimezoneAsRegion));
      properties.put("includeSynonyms", Boolean.toString(this.includeSynonyms));
      PasswordCredentials credsFromCredentialsService = null;
      if (!Strings.isNullOrEmpty(this.secretResourceUrl)) {
         try {
            URI secretURI = URI.create(this.secretResourceUrl);
            credsFromCredentialsService = (PasswordCredentials)credentialsService.getCredentials(secretURI);
         } catch (IOException var11) {
            throw new SQLException(var11.getMessage(), var11);
         }
      }

      String thePassword = credsFromCredentialsService != null ? credsFromCredentialsService.getPassword() : this.password;
      boolean isSslCertConfigured = this.isSslCertConfigured();
      String url;
      String protocol;
      if (!this.useLdap) {
         protocol = this.useSsl ? "TCPS" : "TCP";
         String securityOption = this.useSsl && isSslCertConfigured ? String.format("(SECURITY = (SSL_SERVER_CERT_DN = \"%s\"))", this.sslServerCertDN) : "";
         url = String.format("jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=%s)(HOST=%s)(PORT=%d))(CONNECT_DATA=(SERVICE_NAME=%s))%s)", protocol, this.hostname, portAsInteger, this.instance, securityOption);
         if (this.useKerberos) {
            properties.setProperty("oracle.net.authentication_services", "(KERBEROS5)");
            properties.setProperty("oracle.net.kerberos5_mutual_authentication", "true");
         }
      } else {
         protocol = this.useSsl ? "ldaps" : "ldap";
         url = String.format("jdbc:oracle:thin:@%s://%s:%d/%s,%s", protocol, this.hostname, portAsInteger, this.instance, this.bindDN);
      }

      if (!this.useSsl) {
         properties.put("oracle.net.encryption_client", this.nativeEncryption.name());
      }

      if (isSslCertConfigured) {
         properties.put("oracle.net.ssl_server_dn_match", "true");
      }

      if (null != this.propertyList) {
         this.propertyList.forEach((p) -> {
            properties.put(p.name, p.value);
         });
      }

      return newDataSource(dataSource, url, this.username, thePassword, properties, this.maxIdleConns, this.idleTimeSec);
   }

   private boolean isSslCertConfigured() {
      boolean isSslCertConfigured = false;
      if (this.useSsl && !Strings.isNullOrEmpty(this.sslServerCertDN)) {
         this.checkSSLServerCertDN(this.sslServerCertDN);
         isSslCertConfigured = true;
      }

      return isSslCertConfigured;
   }

   private void checkSSLServerCertDN(String sslServerCertDN) {
      try {
         new X500Principal(sslServerCertDN);
      } catch (IllegalArgumentException var3) {
         throw new IllegalArgumentException(String.format("Server certificate DN '%s' does not respect Oracle syntax", sslServerCertDN), var3);
      }
   }

   private static CloseableDataSource newDataSource(ConnectionPoolDataSource source, String url, String username, String password, Properties properties, int maxIdleConns, int idleTimeSec) throws SQLException {
      try {
         MethodUtils.invokeExactMethod(source, "setURL", new Object[]{url});
         if (properties != null) {
            MethodUtils.invokeExactMethod(source, "setConnectionProperties", new Object[]{properties});
         }

         if (username != null) {
            MethodUtils.invokeExactMethod(source, "setUser", new Object[]{username});
            MethodUtils.invokeExactMethod(source, "setPassword", new Object[]{password});
         }

         return DataSources.newSharedDataSource(source, maxIdleConns, (long)idleTimeSec);
      } catch (InvocationTargetException var9) {
         Throwable cause = var9.getCause();
         if (cause != null) {
            Throwables.throwIfInstanceOf(cause, SQLException.class);
         }

         throw new RuntimeException("Cannot instantiate Oracle datasource", var9);
      } catch (ReflectiveOperationException var10) {
         throw new RuntimeException("Cannot instantiate Oracle datasource", var10);
      }
   }

   public OracleDialect getDialect() {
      return ORACLE_ARP_DIALECT;
   }

   public static OracleConf newMessage() {
      OracleConf result = new OracleConf();
      result.includeSynonyms = true;
      result.mapDateToTimestamp = true;
      return result;
   }

   @VisibleForTesting
   public static OracleDialect getDialectSingleton() {
      return ORACLE_ARP_DIALECT;
   }

   public static enum OracleNativeEncryption {
      @Tag(1)
      @DisplayMetadata(
         label = "Rejected"
      )
      REJECTED,
      @Tag(2)
      @DisplayMetadata(
         label = "Accepted"
      )
      ACCEPTED,
      @Tag(3)
      @DisplayMetadata(
         label = "Requested"
      )
      REQUESTED,
      @Tag(4)
      @DisplayMetadata(
         label = "Required"
      )
      REQUIRED;
   }
}
