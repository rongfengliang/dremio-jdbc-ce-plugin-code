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
import com.dremio.exec.store.jdbc.dialect.MySQLDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.legacy.LegacyCapableJdbcConf;
import com.dremio.exec.store.jdbc.legacy.LegacyDialect;
import com.dremio.exec.store.jdbc.legacy.MySQLLegacyDialect;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.protostuff.Tag;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.ConnectionPoolDataSource;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import org.apache.commons.lang3.reflect.MethodUtils;

@SourceType(
   value = "MYSQL",
   label = "MySQL",
   uiConfig = "mysql-layout.json"
)
public class MySQLConf extends LegacyCapableJdbcConf<MySQLConf> {
   private static final String ARP_FILENAME = "arp/implementation/mysql-arp.yaml";
   private static final MySQLDialect MYSQL_ARP_DIALECT = (MySQLDialect)AbstractArpConf.loadArpFile("arp/implementation/mysql-arp.yaml", MySQLDialect::new);
   private static final String POOLED_DATASOURCE = "org.mariadb.jdbc.MariaDbDataSource";
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
   public String port = "3306";
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
      label = "Net write timeout (in seconds)"
   )
   @NotMetadataImpacting
   public int netWriteTimeout = 60;
   @Tag(9)
   @DisplayMetadata(
      label = "Enable legacy dialect"
   )
   public boolean useLegacyDialect = false;
   @Tag(10)
   @NotMetadataImpacting
   @DisplayMetadata(
      label = "Grant External Query access (Warning: External Query allows users with the Can Query privilege on this source to query any table or view within the source)"
   )
   public boolean enableExternalQuery = false;

   @VisibleForTesting
   public JdbcPluginConfig buildPluginConfig(Builder configBuilder, CredentialsService credentialsService, OptionManager optionManager) {
      return configBuilder.withDialect(this.getDialect()).withDatasourceFactory(this::newDataSource).withShowOnlyConnDatabase(false).withFetchSize(this.fetchSize).withAllowExternalQuery(this.supportsExternalQuery(this.enableExternalQuery)).build();
   }

   private CloseableDataSource newDataSource() throws SQLException {
      ConnectionPoolDataSource source;
      try {
         source = (ConnectionPoolDataSource)Class.forName("org.mariadb.jdbc.MariaDbDataSource").newInstance();
      } catch (ReflectiveOperationException var3) {
         throw new RuntimeException("Cannot instantiate MySQL datasource", var3);
      }

      return this.newDataSource(source);
   }

   @VisibleForTesting
   CloseableDataSource newDataSource(ConnectionPoolDataSource source) throws SQLException {
      String url = this.toJdbcConnectionString();
      Map<String, String> properties = new LinkedHashMap();
      properties.put("useJDBCCompliantTimezoneShift", "true");
      properties.put("sessionVariables", String.format("net_write_timeout=%d", this.netWriteTimeout));
      String encodedProperties = (String)properties.entrySet().stream().map((entry) -> {
         return (String)entry.getKey() + '=' + (String)entry.getValue();
      }).collect(Collectors.joining("&"));

      try {
         MethodUtils.invokeExactMethod(source, "setUrl", new Object[]{url});
         if (this.username != null) {
            MethodUtils.invokeExactMethod(source, "setUser", new Object[]{this.username});
         }

         if (this.password != null) {
            MethodUtils.invokeExactMethod(source, "setPassword", new Object[]{this.password});
         }

         MethodUtils.invokeExactMethod(source, "setProperties", new Object[]{encodedProperties});
         return DataSources.newSharedDataSource(source);
      } catch (InvocationTargetException var7) {
         Throwable cause = var7.getCause();
         if (cause != null) {
            Throwables.throwIfInstanceOf(cause, SQLException.class);
         }

         throw new RuntimeException("Cannot instantiate MySQL datasource", var7);
      } catch (ReflectiveOperationException var8) {
         throw new RuntimeException("Cannot instantiate MySQL datasource", var8);
      }
   }

   private String toJdbcConnectionString() {
      String hostname = (String)Preconditions.checkNotNull(this.hostname, "missing hostname");
      String portAsString = (String)Preconditions.checkNotNull(this.port, "missing port");
      int port = Integer.parseInt(portAsString);
      return String.format("jdbc:mariadb://%s:%d", hostname, port);
   }

   protected LegacyDialect getLegacyDialect() {
      return MySQLLegacyDialect.INSTANCE;
   }

   protected ArpDialect getArpDialect() {
      return MYSQL_ARP_DIALECT;
   }

   @VisibleForTesting
   public static MySQLDialect getDialectSingleton() {
      return MYSQL_ARP_DIALECT;
   }

   protected boolean getLegacyFlag() {
      return this.useLegacyDialect;
   }

   public static MySQLConf newMessage() {
      MySQLConf result = new MySQLConf();
      result.useLegacyDialect = true;
      return result;
   }
}
