package com.dremio.exec.store.jdbc;

import com.dremio.common.AutoCloseables;
import com.dremio.common.dialect.DremioSqlDialect.ContainerSupport;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.util.CloseableIterator;
import com.dremio.exec.store.StoragePluginUtils;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.CanonicalizeTablePathRequest;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.CanonicalizeTablePathResponse;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.CatalogOrSchemaExistsRequest;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.CatalogOrSchemaExistsResponse;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.GetExternalQueryMetadataRequest;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.GetExternalQueryMetadataResponse;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.GetStateRequest;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.GetStateResponse;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.GetTableMetadataRequest;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.GetTableMetadataResponse;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.ListTableNamesRequest;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.GetStateResponse.Status;
import com.dremio.exec.store.jdbc.legacy.JdbcDremioSqlDialect;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableList.Builder;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSchemaFetcherImpl implements JdbcSchemaFetcher {
   private static final Logger logger = LoggerFactory.getLogger(JdbcSchemaFetcherImpl.class);
   protected static final long BIG_ROW_COUNT = 1000000000L;
   protected static final Joiner PERIOD_JOINER = Joiner.on(".");
   protected final JdbcPluginConfig config;
   protected CloseableDataSource dataSource;

   public JdbcSchemaFetcherImpl(JdbcPluginConfig config) {
      this.config = config;
   }

   public void start() throws IOException {
      try {
         this.dataSource = this.config.getDatasourceFactory().newDataSource();
      } catch (SQLException var2) {
         throw new IOException(StoragePluginUtils.generateSourceErrorMessage(this.config.getSourceName(), var2.getMessage()), var2);
      }
   }

   public void close() throws Exception {
      AutoCloseables.close(new AutoCloseable[]{this.dataSource});
   }

   DataSource getSource() {
      Preconditions.checkNotNull(this.dataSource);
      return this.dataSource;
   }

   protected long getRowCount(List<String> tablePath) {
      String quotedPath = this.getQuotedPath(tablePath);
      logger.debug("Getting row count for table {}. ", quotedPath);
      Optional<Long> count = this.executeQueryAndGetFirstLong("select count(*) from " + quotedPath);
      if (count.isPresent()) {
         return (Long)count.get();
      } else {
         logger.debug("There was a problem getting the row count for table {}, using default of {}.", quotedPath, 1000000000L);
         return 1000000000L;
      }
   }

   public CloseableIterator<CanonicalizeTablePathResponse> listTableNames(ListTableNamesRequest request) {
      logger.debug("Getting all tables for plugin '{}'", this.config.getSourceName());
      return new JdbcSchemaFetcherImpl.JdbcTableNamesIterator(this.config.getSourceName(), this.dataSource, this.config);
   }

   public CanonicalizeTablePathResponse canonicalizeTablePath(CanonicalizeTablePathRequest request) {
      try {
         Connection connection = this.dataSource.getConnection();
         Throwable var3 = null;

         CanonicalizeTablePathResponse var4;
         try {
            if (this.usePrepareForColumnMetadata() && this.config.shouldSkipSchemaDiscovery() || this.usePrepareForGetTables()) {
               var4 = this.getTableHandleViaPrepare(request, connection);
               return var4;
            }

            var4 = this.getDatasetHandleViaGetTables(request, connection);
         } catch (Throwable var9) {
            var3 = var9;
            throw var9;
         } finally {
            if (connection != null) {
               $closeResource(var3, connection);
            }

         }

         return var4;
      } catch (SQLException var11) {
         logger.warn("Failed to fetch schema for {}.", request);
         return CanonicalizeTablePathResponse.getDefaultInstance();
      }
   }

   protected boolean usePrepareForColumnMetadata() {
      return false;
   }

   protected boolean usePrepareForGetTables() {
      return false;
   }

   protected final Optional<Long> executeQueryAndGetFirstLong(String sql) {
      try {
         Connection connection = this.dataSource.getConnection();
         Throwable var3 = null;

         try {
            Statement statement = connection.createStatement();
            Throwable var5 = null;

            try {
               statement.setFetchSize(this.config.getFetchSize());
               statement.setQueryTimeout(this.config.getQueryTimeout());
               ResultSet resultSet = statement.executeQuery(sql);
               Throwable var7 = null;

               try {
                  ResultSetMetaData meta = resultSet.getMetaData();
                  int colCount = meta.getColumnCount();
                  Optional numRows;
                  if (colCount == 1 && resultSet.next()) {
                     numRows = resultSet.getLong(1);
                     Optional var12;
                     if (resultSet.wasNull()) {
                        var12 = Optional.empty();
                        return var12;
                     } else {
                        logger.debug("Query `{}` returned {} rows.", sql, Long.valueOf((long)numRows));
                        var12 = Optional.of(Long.valueOf((long)numRows));
                        return var12;
                     }
                  } else {
                     logger.debug("Invalid results returned for `{}`, colCount = {}.", sql, colCount);
                     numRows = Optional.empty();
                     return numRows;
                  }
               } catch (Throwable var37) {
                  var7 = var37;
                  throw var37;
               } finally {
                  if (resultSet != null) {
                     $closeResource(var7, resultSet);
                  }

               }
            } catch (Throwable var39) {
               var5 = var39;
               throw var39;
            } finally {
               if (statement != null) {
                  $closeResource(var5, statement);
               }

            }
         } catch (Throwable var41) {
            var3 = var41;
            throw var41;
         } finally {
            if (connection != null) {
               $closeResource(var3, connection);
            }

         }
      } catch (Exception var43) {
         logger.warn("Took longer than {} seconds to execute query `{}`.", new Object[]{this.config.getQueryTimeout(), sql, var43});
         return Optional.empty();
      }
   }

   protected static String getJoinedSchema(String catalogName, String schemaName, String tableName) {
      List<String> schemaPathThatFailed = new ArrayList();
      if (!Strings.isNullOrEmpty(catalogName)) {
         schemaPathThatFailed.add(catalogName);
      }

      if (!Strings.isNullOrEmpty(schemaName)) {
         schemaPathThatFailed.add(schemaName);
      }

      if (!Strings.isNullOrEmpty(tableName) && !"%".equals(tableName)) {
         schemaPathThatFailed.add(tableName);
      }

      return PERIOD_JOINER.join(schemaPathThatFailed);
   }

   protected static List<String> getSchemas(DatabaseMetaData metaData, String catalogName, JdbcPluginConfig config, List<String> failed) {
      Builder<String> builder = ImmutableList.builder();
      logger.debug("Getting schemas for catalog=[{}].", catalogName);

      try {
         ResultSet getSchemasResultSet = Strings.isNullOrEmpty(catalogName) ? metaData.getSchemas() : metaData.getSchemas(catalogName, (String)null);
         Throwable var6 = null;

         try {
            while(getSchemasResultSet.next()) {
               String schema = getSchemasResultSet.getString(1);
               if (!config.getHiddenSchemas().contains(schema)) {
                  builder.add(schema);
               }
            }
         } catch (Throwable var12) {
            var6 = var12;
            throw var12;
         } finally {
            if (getSchemasResultSet != null) {
               $closeResource(var6, getSchemasResultSet);
            }

         }
      } catch (SQLException var14) {
         failed.add(getJoinedSchema(catalogName, (String)null, (String)null));
      }

      return builder.build();
   }

   protected final String getQuotedPath(List<String> tablePath) {
      String[] pathSegments = (String[])tablePath.stream().map((path) -> {
         return this.config.getDialect().quoteIdentifier(path);
      }).toArray((x$0) -> {
         return new String[x$0];
      });
      SchemaPath key = SchemaPath.getCompoundPath(pathSegments);
      return key.getAsUnescapedPath();
   }

   protected static boolean supportsCatalogs(AbstractDremioSqlDialect dialect, DatabaseMetaData metaData) throws SQLException {
      if (dialect.supportsCatalogs() == ContainerSupport.AUTO_DETECT) {
         return !Strings.isNullOrEmpty(metaData.getCatalogTerm());
      } else {
         return dialect.supportsCatalogs() == ContainerSupport.SUPPORTED;
      }
   }

   protected static boolean supportsCatalogsWithoutSchemas(AbstractDremioSqlDialect dialect, DatabaseMetaData metaData) throws SQLException {
      return supportsCatalogs(dialect, metaData) && !supportsSchemas(dialect, metaData);
   }

   protected static boolean supportsSchemas(AbstractDremioSqlDialect dialect, DatabaseMetaData metaData) throws SQLException {
      if (dialect.supportsSchemas() == ContainerSupport.AUTO_DETECT) {
         return !Strings.isNullOrEmpty(metaData.getSchemaTerm());
      } else {
         return dialect.supportsSchemas() == ContainerSupport.SUPPORTED;
      }
   }

   protected static boolean supportsSchemasWithoutCatalogs(AbstractDremioSqlDialect dialect, DatabaseMetaData metaData) throws SQLException {
      return supportsSchemas(dialect, metaData) && !supportsCatalogs(dialect, metaData);
   }

   private List<String> getCatalogsOrSchemas() {
      if (this.config.showOnlyConnDatabase() && this.config.getDatabase() != null) {
         return ImmutableList.of(this.config.getDatabase());
      } else {
         try {
            Connection connection = this.dataSource.getConnection();
            Throwable var2 = null;

            List var4;
            try {
               DatabaseMetaData metaData = connection.getMetaData();
               if (!supportsSchemasWithoutCatalogs(this.config.getDialect(), metaData)) {
                  Builder<String> catalogs = ImmutableList.builder();
                  logger.debug("Getting catalogs from JDBC source {}", this.config.getSourceName());
                  ResultSet getCatalogsResultSet = metaData.getCatalogs();
                  Throwable var6 = null;

                  try {
                     while(getCatalogsResultSet.next()) {
                        catalogs.add(getCatalogsResultSet.getString(1));
                     }
                  } catch (Throwable var20) {
                     var6 = var20;
                     throw var20;
                  } finally {
                     if (getCatalogsResultSet != null) {
                        $closeResource(var6, getCatalogsResultSet);
                     }

                  }

                  ImmutableList var26 = catalogs.build();
                  return var26;
               }

               var4 = getSchemas(metaData, (String)null, this.config, new ArrayList());
            } catch (Throwable var22) {
               var2 = var22;
               throw var22;
            } finally {
               if (connection != null) {
                  $closeResource(var2, connection);
               }

            }

            return var4;
         } catch (SQLException var24) {
            logger.error("Error getting catalogs", var24);
            throw new RuntimeException(StoragePluginUtils.generateSourceErrorMessage(this.config.getSourceName(), "Exception while fetching catalog information."), var24);
         }
      }
   }

   private List<String> getSchemas(String catalogName) {
      try {
         Connection connection = this.dataSource.getConnection();
         Throwable var3 = null;

         List var4;
         try {
            var4 = getSchemas(connection.getMetaData(), catalogName, this.config, new ArrayList());
         } catch (Throwable var9) {
            var3 = var9;
            throw var9;
         } finally {
            if (connection != null) {
               $closeResource(var3, connection);
            }

         }

         return var4;
      } catch (SQLException var11) {
         logger.error("Error getting schemas", var11);
         throw new RuntimeException(StoragePluginUtils.generateSourceErrorMessage(this.config.getSourceName(), "Exception while fetching schema information."), var11);
      }
   }

   private CanonicalizeTablePathResponse getDatasetHandleViaGetTables(CanonicalizeTablePathRequest request, Connection connection) throws SQLException {
      DatabaseMetaData metaData = connection.getMetaData();
      JdbcSchemaFetcherImpl.FilterDescriptor filter = new JdbcSchemaFetcherImpl.FilterDescriptor(request, supportsCatalogsWithoutSchemas(this.config.getDialect(), metaData));
      ResultSet tablesResult = metaData.getTables(filter.catalogName, filter.schemaName, filter.tableName, (String[])null);
      Throwable var6 = null;

      CanonicalizeTablePathResponse var10;
      try {
         String currSchema;
         do {
            if (!tablesResult.next()) {
               return CanonicalizeTablePathResponse.getDefaultInstance();
            }

            currSchema = tablesResult.getString(2);
         } while(!Strings.isNullOrEmpty(currSchema) && this.config.getHiddenSchemas().contains(currSchema));

         com.dremio.exec.store.jdbc.JdbcFetcherProto.CanonicalizeTablePathResponse.Builder responseBuilder = CanonicalizeTablePathResponse.newBuilder();
         String currCatalog = tablesResult.getString(1);
         if (!Strings.isNullOrEmpty(currCatalog)) {
            responseBuilder.setCatalog(currCatalog);
         }

         if (!Strings.isNullOrEmpty(currSchema)) {
            responseBuilder.setSchema(currSchema);
         }

         responseBuilder.setTable(tablesResult.getString(3));
         var10 = responseBuilder.build();
      } catch (Throwable var14) {
         var6 = var14;
         throw var14;
      } finally {
         if (tablesResult != null) {
            $closeResource(var6, tablesResult);
         }

      }

      return var10;
   }

   private List<String> getEntities(CanonicalizeTablePathRequest request, String pluginName) {
      Builder<String> builder = ImmutableList.builder();
      if (pluginName != null) {
         builder.add(pluginName);
      }

      if (!Strings.isNullOrEmpty(request.getCatalogOrSchema())) {
         builder.add(request.getCatalogOrSchema());
      }

      if (!Strings.isNullOrEmpty(request.getSchema())) {
         builder.add(request.getSchema());
      }

      if (!Strings.isNullOrEmpty(request.getTable())) {
         builder.add(request.getTable());
      }

      return builder.build();
   }

   private CanonicalizeTablePathResponse getTableHandleViaPrepare(CanonicalizeTablePathRequest request, Connection connection) throws SQLException {
      DatabaseMetaData metaData = connection.getMetaData();
      List<String> trimmedList = this.getEntities(request, (String)null);
      PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + this.getQuotedPath(trimmedList));
      Throwable var6 = null;

      CanonicalizeTablePathResponse var10;
      try {
         ResultSetMetaData preparedMetadata = statement.getMetaData();
         if (preparedMetadata.getColumnCount() <= 0) {
            logger.debug("Table has no columns, query is in invalid");
            CanonicalizeTablePathResponse var16 = CanonicalizeTablePathResponse.getDefaultInstance();
            return var16;
         }

         com.dremio.exec.store.jdbc.JdbcFetcherProto.CanonicalizeTablePathResponse.Builder responseBuilder = CanonicalizeTablePathResponse.newBuilder();
         String table;
         if (supportsCatalogs(this.config.getDialect(), metaData)) {
            table = preparedMetadata.getCatalogName(1);
            if (!Strings.isNullOrEmpty(table)) {
               responseBuilder.setCatalog(table);
            }
         }

         if (supportsSchemas(this.config.getDialect(), metaData)) {
            table = preparedMetadata.getSchemaName(1);
            if (!Strings.isNullOrEmpty(table)) {
               responseBuilder.setSchema(table);
            }
         }

         table = preparedMetadata.getTableName(1);
         if (!Strings.isNullOrEmpty(table)) {
            responseBuilder.setTable(table);
            var10 = responseBuilder.build();
            return var10;
         }

         logger.info("Unable to get table handle for {} via prepare, falling back to getTables.", this.getEntities(request, this.config.getSourceName()));
         var10 = this.getDatasetHandleViaGetTables(request, connection);
      } catch (Throwable var14) {
         var6 = var14;
         throw var14;
      } finally {
         if (statement != null) {
            $closeResource(var6, statement);
         }

      }

      return var10;
   }

   public JdbcPluginConfig getConfig() {
      return this.config;
   }

   public GetStateResponse getState(GetStateRequest getStateRequest) {
      com.dremio.exec.store.jdbc.JdbcFetcherProto.GetStateResponse.Builder builder = GetStateResponse.newBuilder();

      try {
         Connection connection = this.dataSource.getConnection();
         Throwable var4 = null;

         try {
            if (connection.isValid(1)) {
               builder.setStatus(Status.GOOD);
            } else {
               builder.setStatus(Status.BAD).setMessage("Connection is not valid.");
            }
         } catch (Throwable var10) {
            var4 = var10;
            throw var10;
         } finally {
            if (connection != null) {
               $closeResource(var4, connection);
            }

         }
      } catch (SQLException var12) {
         logger.error("Connection is not valid.", var12);
         builder.setStatus(Status.BAD);
      }

      return builder.build();
   }

   public CatalogOrSchemaExistsResponse catalogOrSchemaExists(CatalogOrSchemaExistsRequest request) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(request.getCatalogOrSchema()));
      boolean exists = false;

      try {
         if (Strings.isNullOrEmpty(request.getSchema())) {
            exists = this.getCatalogsOrSchemas().contains(request.getCatalogOrSchema());
         } else {
            exists = this.getSchemas(request.getCatalogOrSchema()).contains(request.getSchema());
         }
      } catch (Exception var4) {
         logger.error("Exception caught while checking if container exists.", var4);
      }

      return CatalogOrSchemaExistsResponse.newBuilder().setExists(exists).build();
   }

   public GetExternalQueryMetadataResponse getExternalQueryMetadata(GetExternalQueryMetadataRequest request) {
      return JdbcExternalQueryMetadataUtility.getBatchSchema(this.dataSource, (JdbcDremioSqlDialect)this.config.getDialect(), request, this.config.getSourceName());
   }

   public GetTableMetadataResponse getTableMetadata(GetTableMetadataRequest request) {
      return (new JdbcDatasetMetadata(this, request)).build();
   }

   // $FF: synthetic method
   private static void $closeResource(Throwable x0, AutoCloseable x1) {
      if (x0 != null) {
         try {
            x1.close();
         } catch (Throwable var3) {
            x0.addSuppressed(var3);
         }
      } else {
         x1.close();
      }

   }

   static class JdbcTableNamesIterator extends AbstractIterator<CanonicalizeTablePathResponse> implements CloseableIterator<CanonicalizeTablePathResponse> {
      private final String storagePluginName;
      private final JdbcPluginConfig config;
      private Connection connection;
      private DatabaseMetaData metaData;
      private String[] tableTypes;
      private boolean supportsCatalogs;
      private boolean hasConstantSchema;
      private boolean hasErrorDuringRetrieval;
      private final List<String> failedCatalogOrSchema = new ArrayList();
      private Iterator<String> catalogs;
      private String currentCatalog = null;
      private Iterator<String> schemas = null;
      private String currentSchema = null;
      private ResultSet tablesResult = null;

      JdbcTableNamesIterator(String storagePluginName, DataSource dataSource, JdbcPluginConfig config) {
         this.storagePluginName = storagePluginName;
         this.config = config;
         this.hasErrorDuringRetrieval = false;

         try {
            this.connection = dataSource.getConnection();
            this.metaData = this.connection.getMetaData();
            this.supportsCatalogs = JdbcSchemaFetcherImpl.supportsCatalogs(config.getDialect(), this.metaData);
            if (config.getDatabase() != null && config.showOnlyConnDatabase()) {
               if (this.supportsCatalogs) {
                  this.catalogs = ImmutableList.of(config.getDatabase()).iterator();
               } else if (JdbcSchemaFetcherImpl.supportsSchemasWithoutCatalogs(config.getDialect(), this.metaData)) {
                  this.currentSchema = config.getDatabase();
               }
            }

            if (null == this.catalogs) {
               if (this.supportsCatalogs) {
                  this.catalogs = this.getCatalogs(this.metaData).iterator();
                  if (!this.catalogs.hasNext()) {
                     this.catalogs = Collections.singleton("").iterator();
                  }
               } else {
                  this.catalogs = Collections.singleton((Object)null).iterator();
               }
            }

            this.hasConstantSchema = null != this.currentSchema || !JdbcSchemaFetcherImpl.supportsSchemas(config.getDialect(), this.metaData);
            this.tableTypes = this.getTableTypes(this.metaData);
         } catch (SQLException var5) {
            JdbcSchemaFetcherImpl.logger.error(String.format("Error retrieving all tables for %s", storagePluginName), var5);
            this.catalogs = Collections.emptyIterator();
         }

      }

      protected CanonicalizeTablePathResponse computeNext() {
         while(true) {
            if (this.supportsCatalogs && this.currentCatalog == null) {
               if (!this.catalogs.hasNext()) {
                  return this.end();
               }

               this.currentCatalog = (String)this.catalogs.next();
               this.tablesResult = null;
               if (!this.hasConstantSchema) {
                  this.currentSchema = null;
                  this.schemas = null;
               }
            }

            if (!this.hasConstantSchema && this.currentSchema == null) {
               if (this.schemas == null) {
                  List<String> schemaFailures = new ArrayList();
                  this.schemas = JdbcSchemaFetcherImpl.getSchemas(this.metaData, this.currentCatalog, this.config, schemaFailures).iterator();
                  this.hasErrorDuringRetrieval |= !schemaFailures.isEmpty();
                  if (this.hasErrorDuringRetrieval && JdbcSchemaFetcherImpl.logger.isDebugEnabled()) {
                     this.failedCatalogOrSchema.addAll(schemaFailures);
                  }
               }

               if (!this.schemas.hasNext()) {
                  if (!this.supportsCatalogs) {
                     return this.end();
                  }

                  this.currentCatalog = null;
                  continue;
               }

               this.currentSchema = (String)this.schemas.next();
               this.tablesResult = null;
            }

            try {
               if (this.tablesResult == null) {
                  try {
                     this.tablesResult = this.metaData.getTables(this.currentCatalog, this.currentSchema, (String)null, this.tableTypes);
                  } catch (SQLException var4) {
                     this.hasErrorDuringRetrieval = true;
                     if (JdbcSchemaFetcherImpl.logger.isDebugEnabled()) {
                        this.failedCatalogOrSchema.add(JdbcSchemaFetcherImpl.getJoinedSchema(this.currentCatalog, this.currentSchema, (String)null));
                     }

                     if (!this.hasConstantSchema) {
                        this.currentSchema = null;
                        continue;
                     }

                     if (this.supportsCatalogs) {
                        this.currentCatalog = null;
                        continue;
                     }

                     throw var4;
                  }
               }

               if (this.tablesResult.next()) {
                  com.dremio.exec.store.jdbc.JdbcFetcherProto.CanonicalizeTablePathResponse.Builder response = CanonicalizeTablePathResponse.newBuilder();
                  String currCatalog = this.tablesResult.getString(1);
                  if (!Strings.isNullOrEmpty(currCatalog)) {
                     response.setCatalog(currCatalog);
                  }

                  String currSchema = this.tablesResult.getString(2);
                  if (!Strings.isNullOrEmpty(currSchema)) {
                     response.setSchema(currSchema);
                  }

                  response.setTable(this.tablesResult.getString(3));
                  return response.build();
               }

               this.tablesResult.close();
               if (this.hasConstantSchema) {
                  if (!this.supportsCatalogs) {
                     return this.end();
                  }

                  this.currentCatalog = null;
               } else {
                  this.currentSchema = null;
               }
            } catch (SQLException var5) {
               JdbcSchemaFetcherImpl.logger.error(String.format("Error listing datasets for '%s'", this.storagePluginName), var5);
               return (CanonicalizeTablePathResponse)this.endOfData();
            }
         }
      }

      private CanonicalizeTablePathResponse end() {
         JdbcSchemaFetcherImpl.logger.debug("Done fetching all schema and tables for '{}'.", this.storagePluginName);
         if (this.hasErrorDuringRetrieval) {
            if (JdbcSchemaFetcherImpl.logger.isDebugEnabled()) {
               JdbcSchemaFetcherImpl.logger.debug("Failed to fetch schema for {}.", this.failedCatalogOrSchema);
            } else {
               JdbcSchemaFetcherImpl.logger.warn("Failed to fetch some tables, for more information enable debug logging.");
            }
         }

         return (CanonicalizeTablePathResponse)this.endOfData();
      }

      private List<String> getCatalogs(DatabaseMetaData metaData) {
         Builder catalogs = ImmutableList.builder();

         try {
            ResultSet getCatalogsResultSet = metaData.getCatalogs();
            Throwable var4 = null;

            try {
               while(getCatalogsResultSet.next()) {
                  catalogs.add(getCatalogsResultSet.getString(1));
               }
            } catch (Throwable var10) {
               var4 = var10;
               throw var10;
            } finally {
               if (getCatalogsResultSet != null) {
                  $closeResource(var4, getCatalogsResultSet);
               }

            }
         } catch (SQLException var12) {
            JdbcSchemaFetcherImpl.logger.error(String.format("Failed to get catalogs for plugin '%s'.", this.storagePluginName), var12);
         }

         return catalogs.build();
      }

      private String[] getTableTypes(DatabaseMetaData metaData) {
         if (this.tableTypes != null) {
            return this.tableTypes;
         } else {
            try {
               ResultSet typesResult = metaData.getTableTypes();
               Throwable var3 = null;

               String type;
               try {
                  ArrayList types = Lists.newArrayList();

                  while(typesResult.next()) {
                     type = typesResult.getString(1).trim();
                     if (!this.config.getHiddenTableTypes().contains(type)) {
                        types.add(type);
                     }
                  }

                  if (!types.isEmpty()) {
                     String[] var13 = (String[])types.toArray(new String[0]);
                     return var13;
                  }

                  type = null;
               } catch (Throwable var10) {
                  var3 = var10;
                  throw var10;
               } finally {
                  if (typesResult != null) {
                     $closeResource(var3, typesResult);
                  }

               }

               return type;
            } catch (SQLException var12) {
               JdbcSchemaFetcherImpl.logger.warn("Unable to retrieve list of table types.", var12);
               return null;
            }
         }
      }

      public void close() throws Exception {
         AutoCloseables.close(new AutoCloseable[]{this.tablesResult, this.connection});
      }

      // $FF: synthetic method
      private static void $closeResource(Throwable x0, AutoCloseable x1) {
         if (x0 != null) {
            try {
               x1.close();
            } catch (Throwable var3) {
               x0.addSuppressed(var3);
            }
         } else {
            x1.close();
         }

      }
   }

   protected static class FilterDescriptor {
      private final String catalogName;
      private final String schemaName;
      private final String tableName;

      public FilterDescriptor(CanonicalizeTablePathRequest request, boolean hasCatalogsWithoutSchemas) {
         this.tableName = request.getTable();
         if (!Strings.isNullOrEmpty(request.getSchema())) {
            this.schemaName = request.getSchema();
            this.catalogName = request.getCatalogOrSchema();
         } else {
            this.catalogName = hasCatalogsWithoutSchemas ? request.getCatalogOrSchema() : "";
            this.schemaName = hasCatalogsWithoutSchemas ? "" : request.getCatalogOrSchema();
         }

      }
   }
}
