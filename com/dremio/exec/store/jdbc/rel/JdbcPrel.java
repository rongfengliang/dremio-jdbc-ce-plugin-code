package com.dremio.exec.store.jdbc.rel;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.rel2sql.SqlImplementor.Result;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.common.JdbcRelBase;
import com.dremio.exec.planner.common.JdbcRelImpl;
import com.dremio.exec.planner.common.MoreRelOptUtil.OrderByInSubQueryRemover;
import com.dremio.exec.planner.common.MoreRelOptUtil.SubsetRemover;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.store.jdbc.JdbcGroupScan;
import com.dremio.exec.store.jdbc.conf.DialectConf;
import com.dremio.exec.store.jdbc.dialect.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.proto.JdbcReaderProto;
import com.dremio.exec.store.jdbc.rel2sql.JdbcDremioRelToSqlConverter;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Options
public class JdbcPrel extends JdbcRelBase implements Prel {
   private static final Logger logger = LoggerFactory.getLogger(JdbcPrel.class);
   public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.jdbc.reserve_bytes", Long.MAX_VALUE, 1000000L);
   public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.jdbc.limit_bytes", Long.MAX_VALUE, Long.MAX_VALUE);
   private final String sql;
   private final double rows;
   private final StoragePluginId pluginId;
   private final List<SchemaPath> columns;
   private final BatchSchema schema;
   private final Set<List<String>> tableList;
   private final Set<String> skippedColumns;
   private final Map<String, List<JdbcReaderProto.ColumnProperty>> columnProperties;

   public JdbcPrel(RelOptCluster cluster, RelTraitSet traitSet, JdbcIntermediatePrel prel, FunctionLookupContext context, StoragePluginId pluginId) {
      super(cluster, traitSet, prel.getSubTree());
      this.pluginId = pluginId;
      this.rows = cluster.getMetadataQuery().getRowCount(this.jdbcSubTree);
      RelNode jdbcInput = rewriteJdbcSubtree(this.jdbcSubTree, PrelUtil.getPlannerSettings(cluster).getOptions().getOption(PlannerSettings.JDBC_PUSH_DOWN_PLUS));
      JdbcPrel.TableInfoAccumulator tableListGenerator = new JdbcPrel.TableInfoAccumulator();
      jdbcInput = jdbcInput.accept(tableListGenerator);
      this.tableList = ImmutableSet.copyOf(tableListGenerator.getTableList());
      this.skippedColumns = ImmutableSet.copyOf(tableListGenerator.getSkippedColumns());
      this.columnProperties = ImmutableMap.copyOf(tableListGenerator.getColumnProperties());
      this.rowType = jdbcInput.getRowType();
      DialectConf<?, ?> conf = (DialectConf)pluginId.getConnectionConf();
      JdbcDremioSqlDialect dialect = conf.getDialect();
      JdbcDremioRelToSqlConverter jdbcImplementor = dialect.getConverter();
      Map<String, Map<String, String>> colProperties = new HashMap();
      Iterator var12 = this.columnProperties.keySet().iterator();

      while(var12.hasNext()) {
         String key = (String)var12.next();
         Map<String, String> properties = new HashMap();
         Iterator var15 = ((List)this.columnProperties.get(key)).iterator();

         while(var15.hasNext()) {
            JdbcReaderProto.ColumnProperty prop = (JdbcReaderProto.ColumnProperty)var15.next();
            properties.put(prop.getKey(), prop.getValue());
         }

         colProperties.put(key, properties);
      }

      jdbcImplementor.setColumnProperties(colProperties);
      Result result = jdbcImplementor.visitChild(0, jdbcInput);
      SqlPrettyWriter writer = new SqlPrettyWriter(dialect);
      writer.setAlwaysUseParentheses(false);
      writer.setSelectListItemsOnSeparateLines(false);
      writer.setIndentation(0);
      result.asQueryOrValues().unparse(writer, 0, 0);
      this.sql = writer.toString();
      Preconditions.checkState(this.sql != null && !this.sql.isEmpty(), "JDBC pushdown sql string cannot be empty");
      this.columns = new ArrayList();
      Iterator var19 = this.rowType.getFieldNames().iterator();

      while(var19.hasNext()) {
         String colNames = (String)var19.next();
         this.columns.add(SchemaPath.getSimplePath(colNames));
      }

      this.schema = this.getSchema(jdbcInput, context);
   }

   public static RelNode rewriteJdbcSubtree(RelNode root, boolean addIdentityProjects) {
      RelNode rewrite1 = root.accept(new SubsetRemover());
      RelNode rewrite2 = rewrite1.accept(new OrderByInSubQueryRemover(rewrite1));
      return addIdentityProjects ? rewrite2.accept(new JdbcPrel.AddIdentityProjectsOnJoins()) : rewrite2;
   }

   public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
      return new JdbcGroupScan(creator.props(this, "$dremio$", this.schema, RESERVE, LIMIT), this.sql, this.columns, this.pluginId, this.schema, this.tableList, this.skippedColumns, this.columnProperties);
   }

   public RelWriter explainTerms(RelWriter pw) {
      return super.explainTerms(pw).item("sql", this.sql);
   }

   public double estimateRowCount(RelMetadataQuery mq) {
      return this.rows;
   }

   public Iterator<Prel> iterator() {
      return Collections.emptyIterator();
   }

   public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
      return logicalVisitor.visitPrel(this, value);
   }

   public SelectionVectorMode[] getSupportedEncodings() {
      return SelectionVectorMode.DEFAULT;
   }

   public SelectionVectorMode getEncoding() {
      return SelectionVectorMode.NONE;
   }

   public boolean needsFinalColumnReordering() {
      return false;
   }

   private BatchSchema getSchema(RelNode node, FunctionLookupContext context) {
      if (node == null) {
         return null;
      } else {
         assert node instanceof JdbcRelImpl : "Found non-JdbcRelImpl in a jdbc subtree, " + node;

         return CalciteArrowHelper.fromCalciteRowType(node.getRowType());
      }
   }

   private static class AddIdentityProjectsOnJoins extends StatelessRelShuttleImpl {
      private AddIdentityProjectsOnJoins() {
      }

      protected RelNode visitChild(RelNode parent, int i, RelNode child) {
         RelNode child2 = child.accept(this);
         if (child2 instanceof JdbcJoin && !(parent instanceof JdbcProject)) {
            JdbcJoin join = (JdbcJoin)child2;
            child2 = new JdbcProject(join.getCluster(), join.getTraitSet(), join, join.getCluster().getRexBuilder().identityProjects(join.getRowType()), join.getRowType(), join.getPluginId());
         }

         if (child2 != child) {
            List<RelNode> newInputs = new ArrayList(parent.getInputs());
            newInputs.set(i, child2);
            return parent.copy(parent.getTraitSet(), newInputs);
         } else {
            return parent;
         }
      }

      // $FF: synthetic method
      AddIdentityProjectsOnJoins(Object x0) {
         this();
      }
   }

   @VisibleForTesting
   public static class TableInfoAccumulator extends StatelessRelShuttleImpl {
      private final Set<List<String>> tableList = Sets.newHashSet();
      private final Set<String> skippedColumns = Sets.newHashSet();
      private final Map<String, List<JdbcReaderProto.ColumnProperty>> columnProperties = new HashMap();

      public RelNode visit(TableScan scan) {
         if (scan instanceof JdbcTableScan) {
            JdbcTableScan tableScan = (JdbcTableScan)scan;
            this.tableList.add(scan.getTable().getQualifiedName());
            ReadDefinition readDefinition = tableScan.getTableMetadata().getReadDefinition();
            if (readDefinition.getExtendedProperty() != null) {
               try {
                  JdbcReaderProto.JdbcTableXattr attrs = JdbcReaderProto.JdbcTableXattr.parseFrom(readDefinition.getExtendedProperty().asReadOnlyByteBuffer());
                  this.skippedColumns.addAll(attrs.getSkippedColumnsList());
                  Iterator var5 = attrs.getColumnPropertiesList().iterator();

                  while(var5.hasNext()) {
                     JdbcReaderProto.ColumnProperties colProp = (JdbcReaderProto.ColumnProperties)var5.next();
                     this.columnProperties.put(colProp.getColumnName(), colProp.getPropertiesList());
                  }

                  Set<String> projected = new HashSet(tableScan.getRowType().getFieldNames());
                  Set<String> skipped = new HashSet(attrs.getSkippedColumnsList());
                  if (projected.size() == 0) {
                     List<SchemaPath> columns = (List)tableScan.getTable().getRowType().getFieldNames().stream().filter((field) -> {
                        return !skipped.contains(field);
                     }).map(SchemaPath::getSimplePath).collect(Collectors.toList());
                     JdbcTableScan newTableScan = (JdbcTableScan)tableScan.cloneWithProject(columns);
                     return new JdbcProject(newTableScan.getCluster(), newTableScan.getTraitSet(), newTableScan, newTableScan.getCluster().getRexBuilder().identityProjects(newTableScan.getRowType()), newTableScan.getRowType(), newTableScan.getPluginId(), true);
                  }

                  if (tableScan.getTable().getRowType().getFieldNames().stream().anyMatch((field) -> {
                     return !projected.contains(field) && !skipped.contains(field);
                  })) {
                     return new JdbcProject(tableScan.getCluster(), tableScan.getTraitSet(), tableScan, tableScan.getCluster().getRexBuilder().identityProjects(tableScan.getRowType()), tableScan.getRowType(), tableScan.getPluginId());
                  }
               } catch (InvalidProtocolBufferException var9) {
                  JdbcPrel.logger.warn("Unable to get extended properties for table {}.", tableScan.getTableName(), var9);
               }
            }
         }

         return super.visit(scan);
      }

      Set<List<String>> getTableList() {
         return this.tableList;
      }

      Set<String> getSkippedColumns() {
         return this.skippedColumns;
      }

      Map<String, List<JdbcReaderProto.ColumnProperty>> getColumnProperties() {
         return this.columnProperties;
      }
   }
}
