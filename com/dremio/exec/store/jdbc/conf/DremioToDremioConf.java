package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcPluginConfig.Builder;
import com.dremio.exec.store.jdbc.dialect.DremioToDremioDialect;
import com.dremio.exec.store.jdbc.dremiotodremio.DremioToDremioPoolDataSource;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import com.google.common.annotations.VisibleForTesting;
import io.protostuff.Tag;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

@SourceType(
   value = "DREMIOTODREMIO",
   label = "Dremio-to-Dremio (preview)",
   uiConfig = "dremio-to-dremio-layout.json",
   externalQuerySupported = true
)
public class DremioToDremioConf extends AbstractArpConf<DremioToDremioConf> {
   private static final String ARP_FILENAME = "arp/implementation/dremio-to-dremio-arp.yaml";
   private static final DremioToDremioDialect DREMIO_TO_DREMIO_ARP_DIALECT = (DremioToDremioDialect)AbstractArpConf.loadArpFile("arp/implementation/dremio-to-dremio-arp.yaml", DremioToDremioDialect::new);
   @Tag(1)
   @DisplayMetadata(
      label = "Endpoint Type"
   )
   public DremioToDremioConf.HostType hostType;
   @NotBlank
   @Tag(2)
   @DisplayMetadata(
      label = "Host"
   )
   public String hostname;
   @NotBlank
   @Tag(3)
   @Min(1L)
   @Max(65535L)
   @DisplayMetadata(
      label = "Port"
   )
   public String port;
   @Tag(4)
   @DisplayMetadata(
      label = "Username"
   )
   public String username;
   @Tag(5)
   @DisplayMetadata(
      label = "Password"
   )
   @Secret
   public String password;
   @Tag(6)
   @DisplayMetadata(
      label = "Use SSL"
   )
   public boolean useSsl;
   @Tag(7)
   @DisplayMetadata(
      label = "User Impersonation"
   )
   public boolean userImpersonation;
   @Tag(8)
   @DisplayMetadata(
      label = "Maximum idle connections"
   )
   @NotMetadataImpacting
   public int maxIdleConns;
   @Tag(9)
   @DisplayMetadata(
      label = "Connection idle time (s)"
   )
   @NotMetadataImpacting
   public int idleTimeSec;
   @Tag(10)
   @DisplayMetadata(
      label = "Query timeout (s)"
   )
   @NotMetadataImpacting
   public int queryTimeoutSec;

   public DremioToDremioConf() {
      this.hostType = DremioToDremioConf.HostType.DIRECT;
      this.port = "31010";
      this.maxIdleConns = 8;
      this.idleTimeSec = 60;
      this.queryTimeoutSec = 0;
   }

   private CloseableDataSource newDataSource() {
      DremioToDremioPoolDataSource dataSource = new DremioToDremioPoolDataSource(this);
      return CloseableDataSource.wrap(dataSource);
   }

   public JdbcPluginConfig buildPluginConfig(Builder configBuilder, CredentialsService credentialsService, OptionManager optionManager) {
      return configBuilder.withDialect(this.getDialect()).withDatasourceFactory(this::newDataSource).withShowOnlyConnDatabase(true).withQueryTimeout(this.queryTimeoutSec).build();
   }

   public DremioToDremioDialect getDialect() {
      return DREMIO_TO_DREMIO_ARP_DIALECT;
   }

   @VisibleForTesting
   public static DremioToDremioDialect getDialectSingleton() {
      return DREMIO_TO_DREMIO_ARP_DIALECT;
   }

   public static enum HostType {
      @Tag(0)
      DIRECT,
      @Tag(1)
      ZOOKEEPER;
   }
}
