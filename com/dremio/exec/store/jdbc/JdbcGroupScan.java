package com.dremio.exec.store.jdbc;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.EndpointAffinity;
import com.dremio.exec.physical.base.AbstractBase;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.fragment.ExecutionNodeMap;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.jdbc.proto.JdbcReaderProto;
import com.dremio.exec.store.schedule.CompleteWork;
import com.dremio.exec.store.schedule.SimpleCompleteWork;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@JsonTypeName("jdbc-scan")
public class JdbcGroupScan extends AbstractBase implements GroupScan<CompleteWork> {
   private final String sql;
   private final List<SchemaPath> columns;
   private final StoragePluginId pluginId;
   private final BatchSchema schema;
   private final Set<List<String>> tableList;
   private final Set<String> skippedColumns;
   private final Map<String, List<JdbcReaderProto.ColumnProperty>> columnProperties;

   @JsonCreator
   public JdbcGroupScan(@JsonProperty("opprops") OpProps props, @JsonProperty("sql") String sql, @JsonProperty("columns") List<SchemaPath> columns, @JsonProperty("config") StoragePluginConfig config, @JsonProperty("pluginId") StoragePluginId pluginId, @JsonProperty("fullSchema") BatchSchema fullSchema, @JsonProperty("tableList") Set<List<String>> tableList, @JsonProperty("skipped-columns") Set<String> skippedColumns, @JsonProperty("column-properties") Map<String, List<JdbcReaderProto.ColumnProperty>> columnProperties) {
      this(props, sql, columns, pluginId, fullSchema, tableList, skippedColumns, columnProperties);
   }

   public JdbcGroupScan(OpProps props, String sql, List<SchemaPath> columns, StoragePluginId pluginId, BatchSchema schema, Set<List<String>> tableList, Set<String> skippedColumns, Map<String, List<JdbcReaderProto.ColumnProperty>> columnProperties) {
      super(props);
      this.sql = sql;
      this.columns = columns;
      this.pluginId = pluginId;
      this.schema = schema;
      this.tableList = tableList;
      this.skippedColumns = skippedColumns;
      this.columnProperties = columnProperties;
   }

   public JdbcGroupScan(OpProps props, String sql, List<SchemaPath> columns, StoragePluginId pluginId, BatchSchema schema, Set<String> skippedColumns) {
      this(props, sql, columns, pluginId, schema, Collections.emptySet(), skippedColumns, Collections.emptyMap());
   }

   public Set<List<String>> getReferencedTables() {
      return this.tableList;
   }

   @JsonIgnore
   public int getMaxParallelizationWidth() {
      return 1;
   }

   public Iterator<PhysicalOperator> iterator() {
      return Collections.emptyIterator();
   }

   public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
      return physicalVisitor.visitGroupScan(this, value);
   }

   public List<SchemaPath> getColumns() {
      return this.columns;
   }

   public Iterator<CompleteWork> getSplits(ExecutionNodeMap nodeMap) {
      return Iterators.singletonIterator(new SimpleCompleteWork(1L, new EndpointAffinity[0]));
   }

   @JsonIgnore
   public int getMinParallelizationWidth() {
      return 1;
   }

   public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
      Preconditions.checkArgument(children == null || children.isEmpty());
      return new JdbcGroupScan(this.props, this.sql, this.columns, this.pluginId, this.props.getSchema(), this.tableList, this.skippedColumns, this.columnProperties);
   }

   public String getSql() {
      return this.sql;
   }

   public JdbcSubScan getSpecificScan(List<CompleteWork> work) throws ExecutionSetupException {
      return new JdbcSubScan(this.props, this.sql, this.getColumns(), this.pluginId, this.schema, this.getReferencedTables(), this.skippedColumns);
   }

   public int getOperatorType() {
      return 47;
   }

   public DistributionAffinity getDistributionAffinity() {
      return DistributionAffinity.NONE;
   }

   public boolean equals(Object o) {
      if (this == o) {
         return true;
      } else if (o != null && this.getClass() == o.getClass()) {
         JdbcGroupScan that = (JdbcGroupScan)o;
         return Objects.equal(this.sql, that.sql) && Objects.equal(this.columns, that.columns) && Objects.equal(this.pluginId.getConfig(), that.pluginId.getConfig()) && Objects.equal(this.schema, that.schema) && Objects.equal(this.tableList, that.tableList) && Objects.equal(this.skippedColumns, that.skippedColumns) && Objects.equal(this.columnProperties, that.columnProperties);
      } else {
         return false;
      }
   }

   public int hashCode() {
      return Objects.hashCode(new Object[]{this.sql, this.columns == null ? 0 : this.columns.size(), this.pluginId.getConfig(), this.schema, this.tableList, this.skippedColumns, this.columnProperties});
   }
}
