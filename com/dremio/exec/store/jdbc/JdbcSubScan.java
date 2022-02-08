package com.dremio.exec.store.jdbc;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@JsonTypeName("jdbc-sub-scan")
public class JdbcSubScan extends SubScanWithProjection {
   private final String sql;
   private final List<SchemaPath> columns;
   private final StoragePluginId pluginId;
   private final Set<String> skippedColumns;

   @JsonCreator
   public JdbcSubScan(@JsonProperty("props") OpProps props, @JsonProperty("sql") String sql, @JsonProperty("columns") List<SchemaPath> columns, @JsonProperty("pluginId") StoragePluginId pluginId, @JsonProperty("fullSchema") BatchSchema fullSchema, @JsonProperty("referenced-tables") Collection<List<String>> tableList, @JsonProperty("skipped-columns") Set<String> skippedColumns) throws ExecutionSetupException {
      super(props, fullSchema, tableList, columns);
      Preconditions.checkArgument(sql != null && !sql.isEmpty(), "JDBC pushdown SQL string cannot be empty in JdbcSubScan");
      this.sql = sql;
      this.columns = columns;
      this.pluginId = pluginId;
      this.skippedColumns = skippedColumns;
   }

   public int getOperatorType() {
      return 47;
   }

   public String getSql() {
      return this.sql;
   }

   public List<SchemaPath> getColumns() {
      return this.columns;
   }

   public StoragePluginId getPluginId() {
      return this.pluginId;
   }

   public Set<String> getSkippedColumns() {
      return this.skippedColumns;
   }
}
