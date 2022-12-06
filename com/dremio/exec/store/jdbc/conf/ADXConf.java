package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcPluginConfig.Builder;
import com.dremio.exec.store.jdbc.dialect.ADXDialect;
import com.dremio.options.OptionManager;
import com.dremio.security.CredentialsService;
import com.google.common.annotations.VisibleForTesting;
import com.microsoft.aad.adal4j.AuthenticationCallback;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import io.protostuff.Tag;
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.PooledConnection;
import javax.validation.constraints.NotBlank;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SourceType(
   value = "ADX",
   label = "Microsoft Azure Data Explorer",
   uiConfig = "adx-layout.json",
   externalQuerySupported = true,
   previewEngineRequired = true
)
public class ADXConf extends AbstractArpConf<ADXConf> {
   private static final Logger logger = LoggerFactory.getLogger(ADXConf.class);
   private static final String ARP_FILENAME = "arp/implementation/adx-arp.yaml";
   private static final ADXDialect ADX_ARP_DIALECT = (ADXDialect)AbstractArpConf.loadArpFile("arp/implementation/adx-arp.yaml", ADXDialect::new);
   private static final int ACCESS_TOKEN_EXPIRY_MINUTES = 5;
   @NotBlank
   @Tag(1)
   @DisplayMetadata(
      label = "Cluster URI"
   )
   public String clusterUri;
   @NotBlank
   @Tag(2)
   @DisplayMetadata(
      label = "Tenant ID"
   )
   public String tenantId;
   @NotBlank
   @Tag(3)
   @DisplayMetadata(
      label = "Application ID"
   )
   public String appId;
   @NotBlank
   @Tag(4)
   @Secret
   @DisplayMetadata(
      label = "Application Secret"
   )
   public String appSecret;
   @NotBlank
   @Tag(5)
   @DisplayMetadata(
      label = "Database Name"
   )
   public String databaseName;
   @Tag(6)
   @DisplayMetadata(
      label = "Maximum idle connections"
   )
   @NotMetadataImpacting
   public int maxIdleConns = 8;
   @Tag(7)
   @DisplayMetadata(
      label = "Connection idle time (s)"
   )
   @NotMetadataImpacting
   public int idleTimeSec = 60;
   @Tag(8)
   @DisplayMetadata(
      label = "Query timeout (s)"
   )
   @NotMetadataImpacting
   public int queryTimeoutSec = 0;

   private CloseableDataSource newDataSource() {
      try {
         SQLServerConnectionPoolDataSource source = new SQLServerConnectionPoolDataSource() {
            private long accessTokenRetrievalTime = 0L;

            public PooledConnection getPooledConnection(String user, String password) throws SQLException {
               if (System.currentTimeMillis() - this.accessTokenRetrievalTime > TimeUnit.MINUTES.toMillis(5L)) {
                  String accessToken = ADXConf.this.getAccessToken();
                  this.accessTokenRetrievalTime = System.currentTimeMillis();
                  this.setAccessToken(accessToken);
               }

               return super.getPooledConnection(user, password);
            }
         };
         source.setServerName(this.getServerName());
         source.setDatabaseName(this.databaseName);
         source.setHostNameInCertificate("*.kusto.windows.net");
         return DataSources.newSharedDataSource(source, this.maxIdleConns, (long)this.idleTimeSec);
      } catch (Exception var2) {
         throw new RuntimeException("Cannot instantiate ADX datasource", var2);
      }
   }

   private String getServerName() {
      if (this.clusterUri.startsWith("http://")) {
         return this.clusterUri.substring(7);
      } else {
         return this.clusterUri.startsWith("https://") ? this.clusterUri.substring(8) : this.clusterUri;
      }
   }

   private String getAccessToken() {
      ExecutorService service = null;

      ClientCredential clientCredential;
      try {
         service = Executors.newSingleThreadExecutor();
         AuthenticationContext authContext = new AuthenticationContext("https://login.microsoftonline.com/" + this.tenantId, false, service);
         clientCredential = new ClientCredential(this.appId, this.appSecret);
         Future<AuthenticationResult> futureAuthResult = authContext.acquireToken(this.clusterUri, clientCredential, (AuthenticationCallback)null);
         AuthenticationResult authResult = (AuthenticationResult)futureAuthResult.get();
         String var6 = authResult.getAccessToken();
         return var6;
      } catch (InterruptedException | ExecutionException | MalformedURLException var10) {
         logger.warn("Unable to get access token for ADX data source.", var10);
         clientCredential = null;
      } finally {
         if (service != null) {
            service.shutdown();
         }

      }

      return clientCredential;
   }

   public JdbcPluginConfig buildPluginConfig(Builder configBuilder, CredentialsService credentialsService, OptionManager optionManager) {
      return configBuilder.withDialect(this.getDialect()).withDatasourceFactory(this::newDataSource).withShowOnlyConnDatabase(true).withDatabase(this.databaseName).withQueryTimeout(this.queryTimeoutSec).build();
   }

   public ADXDialect getDialect() {
      return ADX_ARP_DIALECT;
   }

   @VisibleForTesting
   public static ADXDialect getDialectSingleton() {
      return ADX_ARP_DIALECT;
   }
}
