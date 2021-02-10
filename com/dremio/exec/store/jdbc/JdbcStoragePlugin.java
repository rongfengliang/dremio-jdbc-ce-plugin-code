package com.dremio.exec.store.jdbc;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.util.CloseableIterator;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EmptyDatasetHandleListing;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.SourceMetadata;
import com.dremio.connector.metadata.extensions.SupportsListingDatasets;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.SupportsExternalQuery;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.CanonicalizeTablePathRequest;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.CanonicalizeTablePathResponse;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.CatalogOrSchemaExistsRequest;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.GetExternalQueryMetadataRequest;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.GetExternalQueryMetadataResponse;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.GetStateRequest;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.GetStateResponse;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.GetTableMetadataRequest;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.GetTableMetadataResponse;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.ListTableNamesRequest;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.CatalogOrSchemaExistsRequest.Builder;
import com.dremio.exec.store.jdbc.legacy.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.legacy.LegacyDialect;
import com.dremio.exec.store.jdbc.proto.JdbcReaderProto;
import com.dremio.exec.store.jdbc.rel.JdbcPrel;
import com.dremio.exec.tablefunctions.DremioCalciteResource;
import com.dremio.exec.tablefunctions.ExternalQueryScanPrel;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.BooleanCapability;
import com.dremio.service.namespace.capabilities.BooleanCapabilityValue;
import com.dremio.service.namespace.capabilities.CapabilityValue;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import java.io.IOException;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Provider;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.schema.Function;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcStoragePlugin implements StoragePlugin, SourceMetadata, SupportsListingDatasets, SupportsExternalQuery {
   private static final Logger LOGGER = LoggerFactory.getLogger(JdbcStoragePlugin.class);
   private static final DatasetStats JDBC_STATS;
   public static final BooleanCapability REQUIRE_TRIMS_ON_CHARS;
   public static final BooleanCapability COERCE_TIMES_TO_UTC;
   public static final BooleanCapability COERCE_TIMESTAMPS_TO_UTC;
   public static final BooleanCapability ADJUST_DATE_TIMEZONE;
   private final JdbcPluginConfig config;
   private final SabotConfig sabotConfig;
   private final Provider<StoragePluginId> pluginIdProvider;
   private final boolean enableComplexType;
   private final JdbcSchemaFetcher fetcher;
   private JdbcDremioSqlDialect dialect;
   private boolean started = false;

   public JdbcStoragePlugin(JdbcPluginConfig config, JdbcSchemaFetcher fetcher, SabotConfig sabotConfig, Provider<StoragePluginId> pluginIdProvider, boolean enableComplexType) {
      this.config = config;
      this.fetcher = fetcher;
      this.sabotConfig = sabotConfig;
      this.pluginIdProvider = pluginIdProvider;
      this.enableComplexType = enableComplexType;
      this.dialect = (JdbcDremioSqlDialect)config.getDialect();
   }

   public boolean containerExists(EntityPath containerPath) {
      List<String> components = containerPath.getComponents();
      Builder requestBuilder = CatalogOrSchemaExistsRequest.newBuilder();
      switch(components.size()) {
      case 2:
         requestBuilder.setCatalogOrSchema((String)components.get(1));
         break;
      case 3:
         requestBuilder.setCatalogOrSchema((String)components.get(1)).setSchema((String)components.get(2));
         break;
      default:
         return false;
      }

      return this.fetcher.catalogOrSchemaExists(requestBuilder.build()).getExists();
   }

   public DatasetHandleListing listDatasetHandles(GetDatasetOption... options) {
      if (this.config.shouldSkipSchemaDiscovery()) {
         LOGGER.debug("Skip schema discovery enabled, skipping getting tables '{}'", this.config.getSourceName());
         return new EmptyDatasetHandleListing();
      } else {
         return new JdbcStoragePlugin.JdbcIteratorListing();
      }
   }

   public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options) {
      List<String> components = datasetPath.getComponents();
      com.dremio.exec.store.jdbc.JdbcFetcherProto.CanonicalizeTablePathRequest.Builder requestBuilder = CanonicalizeTablePathRequest.newBuilder();
      switch(components.size()) {
      case 2:
         requestBuilder.setTable((String)components.get(1));
         break;
      case 3:
         requestBuilder.setCatalogOrSchema((String)components.get(1));
         requestBuilder.setTable((String)components.get(2));
         break;
      case 4:
         requestBuilder.setCatalogOrSchema((String)components.get(1));
         requestBuilder.setSchema((String)components.get(2));
         requestBuilder.setTable((String)components.get(3));
         break;
      default:
         return Optional.empty();
      }

      CanonicalizeTablePathResponse tableHandleResponse = this.fetcher.canonicalizeTablePath(requestBuilder.build());
      return !Strings.isNullOrEmpty(tableHandleResponse.getTable()) ? Optional.of(new JdbcStoragePlugin.JdbcDatasetHandle(tableHandleResponse.getCatalog(), tableHandleResponse.getSchema(), tableHandleResponse.getTable())) : Optional.empty();
   }

   public DatasetMetadata getDatasetMetadata(DatasetHandle datasetHandle, PartitionChunkListing chunkListing, GetMetadataOption... options) {
      GetTableMetadataResponse response = ((JdbcStoragePlugin.JdbcDatasetHandle)datasetHandle.unwrap(JdbcStoragePlugin.JdbcDatasetHandle.class)).getTableMetadataResponse();
      List<JdbcReaderProto.ColumnProperties> columnProperties = (List)response.getColumnPropertiesList().stream().map(ColumnPropertiesProcessors::convert).collect(Collectors.toList());
      BytesOutput bytesOutput = (os) -> {
         os.write(JdbcReaderProto.JdbcTableXattr.newBuilder().addAllSkippedColumns(response.getSkippedColumnsList()).addAllColumnProperties(columnProperties).build().toByteArray());
      };
      return DatasetMetadata.of(JDBC_STATS, Schema.deserialize(response.getRecordSchema().asReadOnlyByteBuffer()), bytesOutput);
   }

   public PartitionChunkListing listPartitionChunks(DatasetHandle datasetHandle, ListPartitionChunkOption... options) {
      GetTableMetadataResponse response = ((JdbcStoragePlugin.JdbcDatasetHandle)datasetHandle.unwrap(JdbcStoragePlugin.JdbcDatasetHandle.class)).getTableMetadataResponse();
      Set<PartitionChunk> singleton = Collections.singleton(PartitionChunk.of(new DatasetSplit[]{DatasetSplit.of(Long.MAX_VALUE, response.getRecordCount())}));
      Objects.requireNonNull(singleton);
      return singleton::iterator;
   }

   public SourceCapabilities getSourceCapabilities() {
      return this.dialect == null ? new SourceCapabilities(new CapabilityValue[0]) : new SourceCapabilities(new CapabilityValue[]{new BooleanCapabilityValue(SourceCapabilities.TREAT_CALCITE_SCAN_COST_AS_INFINITE, true), new BooleanCapabilityValue(SourceCapabilities.SUBQUERY_PUSHDOWNABLE, this.dialect.supportsSubquery()), new BooleanCapabilityValue(SourceCapabilities.CORRELATED_SUBQUERY_PUSHDOWN, this.dialect.supportsCorrelatedSubquery()), new BooleanCapabilityValue(REQUIRE_TRIMS_ON_CHARS, this.dialect.requiresTrimOnChars()), new BooleanCapabilityValue(COERCE_TIMES_TO_UTC, this.dialect.coerceTimesToUTC()), new BooleanCapabilityValue(COERCE_TIMESTAMPS_TO_UTC, this.dialect.coerceTimestampsToUTC()), new BooleanCapabilityValue(ADJUST_DATE_TIMEZONE, this.dialect.adjustDateTimezone())});
   }

   public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
      return this.sabotConfig.getClass("dremio.plugins.jdbc.rulesfactory", StoragePluginRulesFactory.class, JdbcRulesFactory.class);
   }

   public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
      return true;
   }

   public SourceState getState() {
      if (!this.started) {
         LOGGER.error("JDBC source {} has not been started.", this.config.getSourceName());
         return SourceState.badState(String.format("Could not connect to %s, check your JDBC connection information and credentials", this.config.getSourceName()), new String[]{String.format("JDBC source %s has not been started.", this.config.getSourceName())});
      } else {
         GetStateResponse response = this.fetcher.getState(GetStateRequest.newBuilder().build());
         switch(response.getStatus()) {
         case GOOD:
            return SourceState.GOOD;
         case BAD:
            return SourceState.badState("", new String[]{response.getMessage()});
         default:
            return SourceState.badState("", new String[0]);
         }
      }
   }

   /** @deprecated */
   @Deprecated
   public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
      return null;
   }

   public void start() throws IOException {
      if (this.dialect == null) {
         throw new RuntimeException("Failure instantiating the dialect for this source. Please see Dremio logs for more information.");
      } else {
         this.fetcher.start();
         this.started = true;
      }
   }

   JdbcSchemaFetcher getFetcher() {
      return this.fetcher;
   }

   JdbcPluginConfig getConfig() {
      return this.config;
   }

   JdbcDremioSqlDialect getDialect() {
      return this.dialect;
   }

   private StoragePluginId getPluginId() {
      return (StoragePluginId)this.pluginIdProvider.get();
   }

   public void close() throws Exception {
      this.fetcher.close();
   }

   private BatchSchema getExternalQuerySchema(String query) {
      GetExternalQueryMetadataRequest request = GetExternalQueryMetadataRequest.newBuilder().setSql(query).build();
      GetExternalQueryMetadataResponse response = this.fetcher.getExternalQueryMetadata(request);
      return BatchSchema.deserialize(response.getBatchSchema().toByteArray());
   }

   public List<Function> getFunctions(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
      String sourceName = this.getPluginId().getName();
      if (this.config.allowExternalQuery()) {
         return (List)SupportsExternalQuery.getExternalQueryFunction((query) -> {
            return this.getExternalQuerySchema(query);
         }, (schema) -> {
            return CalciteArrowHelper.wrap(schema).toCalciteRecordType(SqlTypeFactoryImpl.INSTANCE, (f) -> {
               return f.getType().getTypeID() != Null.TYPE_TYPE;
            }, this.enableComplexType);
         }, this.getPluginId(), tableSchemaPath).map(Collections::singletonList).orElse(Collections.emptyList());
      } else {
         String errorMsg = this.dialect instanceof LegacyDialect ? "External Query is not supported with legacy mode enabled on source" : "Permission denied to run External Query on source";
         throw newValidationError(errorMsg, sourceName);
      }
   }

   private static CalciteContextException newValidationError(String errorMsg, String sourceName) {
      return SqlUtil.newContextException(SqlParserPos.ZERO, DremioCalciteResource.DREMIO_CALCITE_RESOURCE.externalQueryNotSupportedError(errorMsg + " <" + sourceName + ">"));
   }

   public PhysicalOperator getExternalQueryPhysicalOperator(PhysicalPlanCreator creator, ExternalQueryScanPrel prel, BatchSchema schema, String sql) {
      SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
      com.google.common.collect.ImmutableSet.Builder<String> skippedColumnsBuilder = new com.google.common.collect.ImmutableSet.Builder();
      this.filterBatchSchema(schema, schemaBuilder, skippedColumnsBuilder);
      BatchSchema filteredSchema = schemaBuilder.build();
      ImmutableSet<String> skippedColumns = skippedColumnsBuilder.build();
      return new JdbcGroupScan(creator.props(prel, "$dremio$", schema, JdbcPrel.RESERVE, JdbcPrel.LIMIT), sql, (List)filteredSchema.getFields().stream().map((f) -> {
         return SchemaPath.getSimplePath(f.getName());
      }).collect(ImmutableList.toImmutableList()), this.getPluginId(), filteredSchema, skippedColumns);
   }

   private void filterBatchSchema(BatchSchema originalSchema, SchemaBuilder filteredSchemaBuilder, com.google.common.collect.ImmutableSet.Builder<String> skippedColumnsBuilder) {
      Iterator var4 = originalSchema.iterator();

      while(var4.hasNext()) {
         Field field = (Field)var4.next();
         if (field.getType().getTypeID() == Null.TYPE_TYPE) {
            skippedColumnsBuilder.add(field.getName());
         } else {
            filteredSchemaBuilder.addField(field);
         }
      }

   }

   static {
      DriverManager.getDrivers();
      JDBC_STATS = DatasetStats.of(ScanCostFactor.JDBC.getFactor());
      REQUIRE_TRIMS_ON_CHARS = new BooleanCapability("require_trims_on_chars", false);
      COERCE_TIMES_TO_UTC = new BooleanCapability("coerce_times_to_utc", false);
      COERCE_TIMESTAMPS_TO_UTC = new BooleanCapability("coerce_timestamps_to_utc", false);
      ADJUST_DATE_TIMEZONE = new BooleanCapability("adjust_date_timezone", false);
   }

   public class JdbcDatasetHandle implements DatasetHandle {
      private final EntityPath entityPath;
      private final String catalog;
      private final String schema;
      private final String table;
      private GetTableMetadataResponse tableMetadataResponse = null;

      JdbcDatasetHandle(String catalog, String schema, String table) {
         this.catalog = catalog;
         this.schema = schema;
         this.table = table;
         com.google.common.collect.ImmutableList.Builder<String> builder = ImmutableList.builder();
         builder.add(JdbcStoragePlugin.this.config.getSourceName());
         if (!Strings.isNullOrEmpty(catalog)) {
            builder.add(catalog);
         }

         if (!Strings.isNullOrEmpty(schema)) {
            builder.add(schema);
         }

         builder.add(table);
         this.entityPath = new EntityPath(builder.build());
      }

      public EntityPath getDatasetPath() {
         return this.entityPath;
      }

      GetTableMetadataResponse getTableMetadataResponse() {
         if (this.tableMetadataResponse == null) {
            this.tableMetadataResponse = JdbcStoragePlugin.this.fetcher.getTableMetadata(GetTableMetadataRequest.newBuilder().setCatalog(this.catalog).setSchema(this.schema).setTable(this.table).build());
         }

         return this.tableMetadataResponse;
      }
   }

   class JdbcIteratorListing implements DatasetHandleListing {
      final Set<CloseableIterator<CanonicalizeTablePathResponse>> references = new HashSet();

      public Iterator<DatasetHandle> iterator() {
         CloseableIterator<CanonicalizeTablePathResponse> iterator = JdbcStoragePlugin.this.fetcher.listTableNames(ListTableNamesRequest.newBuilder().build());
         this.references.add(iterator);
         return Iterators.transform(iterator, (input) -> {
            return JdbcStoragePlugin.this.new JdbcDatasetHandle(input.getCatalog(), input.getSchema(), input.getTable());
         });
      }

      public void close() {
         try {
            AutoCloseables.close(this.references);
         } catch (Exception var2) {
            JdbcStoragePlugin.LOGGER.warn("Error closing iterators when listing JDBC datasets.", var2);
         }

      }
   }
}
