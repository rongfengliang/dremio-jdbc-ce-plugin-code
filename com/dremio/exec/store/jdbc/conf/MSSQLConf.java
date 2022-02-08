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
import com.dremio.exec.store.jdbc.dialect.MSSQLDialect;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import io.protostuff.Tag;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import javax.sql.ConnectionPoolDataSource;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import org.apache.commons.lang3.reflect.MethodUtils;

@SourceType(
   value = "MSSQL",
   label = "Microsoft SQL Server",
   uiConfig = "mssql-layout.json",
   externalQuerySupported = true
)
public class MSSQLConf extends AbstractArpConf<MSSQLConf> {
   private static final String ARP_FILENAME = "arp/implementation/mssql-arp.yaml";
   private static final MSSQLDialect MS_ARP_DIALECT = (MSSQLDialect)AbstractArpConf.loadArpFile("arp/implementation/mssql-arp.yaml", MSSQLDialect::new);
   private static final String POOLED_DATASOURCE = "com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource";
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
   public String port = "1433";
   @Tag(4)
   @DisplayMetadata(
      label = "Username"
   )
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
      label = "Database (optional)"
   )
   public String database;
   @Tag(9)
   @DisplayMetadata(
      label = "Show only the initial database used for connecting"
   )
   public boolean showOnlyConnectionDatabase = false;
   @Tag(10)
   @DisplayMetadata(
      label = "Enable legacy dialect"
   )
   @JsonIgnore
   public boolean useLegacyDialect = false;
   @Tag(11)
   @DisplayMetadata(
      label = "Encrypt connection"
   )
   @NotMetadataImpacting
   public boolean useSsl = false;
   @Tag(12)
   @NotMetadataImpacting
   @DisplayMetadata(
      label = "Verify server certificate"
   )
   public boolean enableServerVerification = true;
   @Tag(13)
   @NotMetadataImpacting
   @DisplayMetadata(
      label = "SSL/TLS server certificate distinguished name"
   )
   public String hostnameOverride;
   @Tag(14)
   @NotMetadataImpacting
   @JsonIgnore
   public boolean enableExternalQuery = false;
   @Tag(15)
   public List<Property> propertyList;
   @Tag(16)
   @DisplayMetadata(
      label = "Maximum idle connections"
   )
   @NotMetadataImpacting
   public int maxIdleConns = 8;
   @Tag(17)
   @DisplayMetadata(
      label = "Connection idle time (s)"
   )
   @NotMetadataImpacting
   public int idleTimeSec = 60;
   @Tag(18)
   @DisplayMetadata(
      label = "Query timeout (s)"
   )
   @NotMetadataImpacting
   public int queryTimeoutSec = 0;

   public JdbcPluginConfig buildPluginConfig(Builder configBuilder, CredentialsService credentialsService, OptionManager optionManager) {
      return configBuilder.withDialect(this.getDialect()).withDatasourceFactory(this::newDataSource).withDatabase(this.database).withShowOnlyConnDatabase(this.showOnlyConnectionDatabase).withFetchSize(this.fetchSize).withQueryTimeout(this.queryTimeoutSec).build();
   }

   private CloseableDataSource newDataSource() throws SQLException {
      String url = this.toJdbcConnectionString();

      try {
         ConnectionPoolDataSource source = (ConnectionPoolDataSource)Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource").newInstance();
         MethodUtils.invokeExactMethod(source, "setURL", new Object[]{url});
         if (this.username != null) {
            MethodUtils.invokeExactMethod(source, "setUser", new Object[]{this.username});
         }

         if (this.password != null) {
            MethodUtils.invokeExactMethod(source, "setPassword", new Object[]{this.password});
         }

         return DataSources.newSharedDataSource(source, this.maxIdleConns, (long)this.idleTimeSec);
      } catch (InvocationTargetException var4) {
         Throwable cause = var4.getCause();
         if (cause != null) {
            Throwables.throwIfInstanceOf(cause, SQLException.class);
         }

         throw new RuntimeException("Cannot instantiate MSSQL datasource", var4);
      } catch (ReflectiveOperationException var5) {
         throw new RuntimeException("Cannot instantiate MSSQL datasource", var5);
      }
   }

   @VisibleForTesting
   String toJdbcConnectionString() {
      String hostname = (String)Preconditions.checkNotNull(this.hostname, "missing hostname");
      StringBuilder urlBuilder = (new StringBuilder("jdbc:sqlserver://")).append(hostname);
      if (!Strings.isNullOrEmpty(this.port)) {
         urlBuilder.append(":").append(this.port);
      }

      if (!Strings.isNullOrEmpty(this.database)) {
         urlBuilder.append(";databaseName=").append(this.database);
      }

      if (this.useSsl) {
         urlBuilder.append(";encrypt=true");
         if (!this.enableServerVerification) {
            urlBuilder.append(";trustServerCertificate=true");
         } else if (!Strings.isNullOrEmpty(this.hostnameOverride)) {
            urlBuilder.append(";hostNameInCertificate=").append(this.hostnameOverride);
         }
      }

      if (null != this.propertyList && !this.propertyList.isEmpty()) {
         urlBuilder.append((String)this.propertyList.stream().map((p) -> {
            return p.name + "=" + p.value;
         }).collect(Collectors.joining(";", ";", "")));
      }

      return urlBuilder.toString();
   }

   public MSSQLDialect getDialect() {
      return MS_ARP_DIALECT;
   }

   @VisibleForTesting
   public static MSSQLDialect getDialectSingleton() {
      return MS_ARP_DIALECT;
   }
}
