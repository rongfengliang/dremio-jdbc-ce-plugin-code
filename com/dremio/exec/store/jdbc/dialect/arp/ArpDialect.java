package com.dremio.exec.store.jdbc.dialect.arp;

import com.dremio.common.AutoCloseables;
import com.dremio.common.dialect.DremioSqlDialect.ContainerSupport;
import com.dremio.common.dialect.arp.transformer.CallTransformer;
import com.dremio.common.dialect.arp.transformer.NoOpTransformer;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.util.CloseableIterator;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcSchemaFetcherImpl;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.CanonicalizeTablePathResponse;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.ListTableNamesRequest;
import com.dremio.exec.store.jdbc.dialect.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.dialect.TypeMapper;
import com.dremio.exec.store.jdbc.dialect.arp.transformer.TimeUnitFunctionTransformer;
import com.dremio.exec.store.jdbc.dialect.arp.transformer.TrimTransformer;
import com.dremio.exec.store.jdbc.rel.JdbcSort;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet.Builder;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriter.Frame;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArpDialect extends JdbcDremioSqlDialect {
   private final ArpYaml yaml;
   private final ArpTypeMapper typeMapper;
   private static final Set<SqlKind> MANDATORY_OPERATORS;
   private static final Logger logger;

   public ArpDialect(ArpYaml yaml) {
      super(yaml.getMetadata().getName(), yaml.getSyntax().getIdentifierQuote(), yaml.getNullCollation());
      this.yaml = yaml;
      this.typeMapper = new ArpTypeMapper(yaml);
   }

   public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      if (!(call.getOperator() instanceof SqlMonotonicBinaryOperator) && call.getOperator() != SqlStdOperatorTable.DIVIDE && call.getOperator() != SqlStdOperatorTable.AND && call.getOperator() != SqlStdOperatorTable.OR) {
         super.unparseCall(writer, call, leftPrec, rightPrec);
      } else {
         Frame frame = writer.startList("(", ")");
         super.unparseCall(writer, call, leftPrec, rightPrec);
         writer.endList(frame);
      }

   }

   public SqlNode emulateNullDirection(SqlNode node, boolean nullsFirst, boolean desc) {
      return this.emulateNullDirectionWithIsNull(node, nullsFirst, desc);
   }

   public boolean shouldInjectNumericCastToProject() {
      return this.yaml.shouldInjectNumericCastToProject();
   }

   public boolean shouldInjectApproxNumericCastToProject() {
      return this.yaml.shouldInjectApproxNumericCastToProject();
   }

   public boolean mapBooleanToBitExpr() {
      return this.yaml.mapBooleanToBitExpr();
   }

   public boolean supportsLiteral(CompleteType type) {
      return this.yaml.supportsLiteral(type);
   }

   public boolean supportsAggregateFunction(SqlKind kind) {
      return true;
   }

   public boolean supportsAggregation() {
      return this.yaml.supportsAggregation();
   }

   public boolean supportsDateTimeFormatString(String dateTimeFormatStr) {
      dateTimeFormatStr = dateTimeFormatStr.replaceAll("\".*?\"+", "");
      Iterator var2 = this.yaml.getDateTimeFormatSupport().getDateTimeFormatMappings().iterator();

      while(true) {
         DateTimeFormatSupport.DateTimeFormatMapping dtFormat;
         do {
            if (!var2.hasNext()) {
               return true;
            }

            dtFormat = (DateTimeFormatSupport.DateTimeFormatMapping)var2.next();
         } while(!dateTimeFormatStr.contains(dtFormat.getDremioDateTimeFormatString()));

         if (null == dtFormat.getSourceDateTimeFormat() || !dtFormat.getSourceDateTimeFormat().isEnable()) {
            return false;
         }

         dateTimeFormatStr = dateTimeFormatStr.replaceAll(dtFormat.getDremioDateTimeFormatString(), "");
      }
   }

   public boolean supportsNumericFormatString(String numericFormatStr) {
      Iterator var2 = this.yaml.getNumericFormatSupport().getNumericFormatMappings().iterator();

      NumericFormatSupport.NumericFormatMapping format;
      do {
         do {
            if (!var2.hasNext()) {
               return true;
            }

            format = (NumericFormatSupport.NumericFormatMapping)var2.next();
         } while(!numericFormatStr.contains(format.getDremioNumericFormatString()));
      } while(null != format.getSourceNumericFormat() && format.getSourceNumericFormat().isEnable());

      return false;
   }

   public boolean supportsDistinct() {
      return this.yaml.supportsDistinct();
   }

   public TypeMapper getDataTypeMapper(JdbcPluginConfig config) {
      return this.typeMapper;
   }

   protected ArpYaml getYaml() {
      return this.yaml;
   }

   public SqlNode getCastSpec(RelDataType type) {
      return this.yaml.getCastSpec(type);
   }

   public boolean supportsFunction(SqlOperator operator, RelDataType type, List<RelDataType> paramTypes) {
      if (!MANDATORY_OPERATORS.contains(operator.getKind()) && (operator != SqlStdOperatorTable.CAST || this.getCastSpec(type) == null)) {
         if (operator.requiresOver()) {
            return super.supportsFunction(operator, type, paramTypes);
         } else if (operator.isAggregator()) {
            return operator.getKind() == SqlKind.COUNT ? this.yaml.supportsCountOperation(operator, paramTypes) : this.yaml.supportsAggregate(operator, false, paramTypes, type);
         } else {
            return this.yaml.supportsScalarOperator(operator, paramTypes, type);
         }
      } else {
         return true;
      }
   }

   public boolean supportsCount(AggregateCall call) {
      return this.yaml.supportsCountOperation(call);
   }

   public boolean supportsFunction(AggregateCall aggCall, List<RelDataType> paramTypes) {
      return this.yaml.supportsAggregate(aggCall.getAggregation(), aggCall.isDistinct(), paramTypes, aggCall.getType());
   }

   public boolean supportsTimeUnitFunction(SqlOperator operator, TimeUnitRange timeUnit, RelDataType returnType, List<RelDataType> paramTypes) {
      return this.yaml.supportsTimeUnitFunction(operator, timeUnit, paramTypes, returnType);
   }

   public boolean supportsSubquery() {
      return this.yaml.supportsSubquery();
   }

   public boolean supportsCorrelatedSubquery() {
      return this.yaml.supportsCorrelatedSubquery();
   }

   public String getValuesDummyTable() {
      return this.yaml.getValuesDummyTable();
   }

   public boolean supportsFetchOffsetInSetOperand() {
      return this.yaml.allowsSortInSetOperand();
   }

   public boolean supportsUnion() {
      return this.yaml.supportsUnion();
   }

   public boolean supportsUnionAll() {
      return this.yaml.supportsUnionAll();
   }

   public boolean supportsSort(boolean isCollationEmpty, boolean isOffsetEmpty) {
      boolean requiresLimit = true;
      boolean requiresOrderBy = !isCollationEmpty;
      boolean requiresOffset = !isOffsetEmpty;
      if (requiresOrderBy && !this.yaml.supportsOrderBy()) {
         return false;
      } else if (requiresOffset && !this.yaml.supportsFetchOffset()) {
         return false;
      } else {
         return requiresOffset || this.yaml.supportsLimit();
      }
   }

   public boolean supportsSort(Sort sort) {
      boolean requiresOrderBy = !JdbcSort.isCollationEmpty(sort);
      boolean requiresOffsetAndFetch = !JdbcSort.isOffsetEmpty(sort) && sort.fetch != null;
      boolean requiresLimitWithoutOffset = sort.fetch != null && JdbcSort.isOffsetEmpty(sort);
      boolean requiresOffsetWithoutLimit = sort.fetch == null && !JdbcSort.isOffsetEmpty(sort);
      return (this.yaml.supportsOrderBy() || !requiresOrderBy) && (this.yaml.supportsFetchOffset() || !requiresOffsetAndFetch) && (this.yaml.supportsLimit() || !requiresLimitWithoutOffset) && (this.yaml.supportsOffset() || !requiresOffsetWithoutLimit);
   }

   public void unparseOffsetFetch(SqlWriter writer, SqlNode offset, SqlNode fetch) {
      SqlPrettyWriter tempWriter = new SqlPrettyWriter(writer.getDialect());
      tempWriter.setAlwaysUseParentheses(false);
      tempWriter.setSelectListItemsOnSeparateLines(false);
      tempWriter.setIndentation(0);
      String offsetString = null;
      String fetchString = null;
      if (offset != null && (!(offset instanceof SqlNumericLiteral) || !((SqlNumericLiteral)offset).bigDecimalValue().equals(BigDecimal.ZERO))) {
         offset.unparse(tempWriter, 0, 0);
         offsetString = tempWriter.toString();
         tempWriter.reset();
      }

      if (fetch != null) {
         fetch.unparse(tempWriter, 0, 0);
         fetchString = tempWriter.toString();
         tempWriter.reset();
      }

      if (offsetString != null && fetchString != null) {
         writer.print(MessageFormat.format(this.yaml.getFetchOffsetFormat(), offsetString, fetchString));
      } else if (offsetString != null) {
         writer.print(MessageFormat.format(this.yaml.getOffsetFormat(), offsetString));
      } else if (fetchString != null) {
         writer.print(MessageFormat.format(this.yaml.getLimitFormat(), fetchString));
      }

   }

   public boolean supportsJoin(JoinType type) {
      return this.yaml.getJoinSupport(type).isEnable();
   }

   public void unparseJoin(SqlWriter writer, SqlJoin join, int leftPrec, int rightPrec) {
      JoinType type = join.getJoinType();
      if (type == JoinType.COMMA) {
         type = JoinType.CROSS;
      }

      String rewriteFormat = this.yaml.getJoinSupport(type).getRewrite();
      if (rewriteFormat == null) {
         super.unparseJoin(writer, join, leftPrec, rightPrec);
      } else {
         SqlPrettyWriter tempWriter = new SqlPrettyWriter(writer.getDialect());
         tempWriter.setAlwaysUseParentheses(false);
         tempWriter.setSelectListItemsOnSeparateLines(false);
         tempWriter.setIndentation(0);
         join.getLeft().unparse(tempWriter, 0, 0);
         String left = tempWriter.toString();
         tempWriter.reset();
         join.getRight().unparse(tempWriter, 0, 0);
         String right = tempWriter.toString();
         tempWriter.reset();
         if (type == JoinType.CROSS) {
            writer.print(MessageFormat.format(rewriteFormat, left, right));
         } else {
            join.getCondition().unparse(tempWriter, 0, 0);
            String condition = tempWriter.toString();
            writer.print(MessageFormat.format(rewriteFormat, left, right, condition));
         }

      }
   }

   public ContainerSupport supportsCatalogs() {
      return this.yaml.getSyntax().supportsCatalogs();
   }

   public ContainerSupport supportsSchemas() {
      return this.yaml.getSyntax().supportsSchemas();
   }

   public boolean supportsNestedAggregations() {
      return false;
   }

   public CallTransformer getCallTransformer(RexCall call) {
      return ArpDialect.ArpCallTransformers.getTransformer(call);
   }

   public CallTransformer getCallTransformer(SqlOperator op) {
      return ArpDialect.ArpCallTransformers.getTransformer(op);
   }

   public boolean hasBooleanLiteralOrRexCallReturnsBoolean(RexNode node, boolean rexCallCanReturnBoolean) {
      SqlTypeName nodeDataType = node.getType().getSqlTypeName();
      if (node instanceof RexLiteral) {
         boolean toReturn = nodeDataType == SqlTypeName.BOOLEAN;
         if (toReturn) {
            logger.debug("Boolean RexLiteral found, {}", node);
         }

         return toReturn;
      } else if (node instanceof RexInputRef) {
         return false;
      } else {
         if (node instanceof RexCall) {
            RexCall call = (RexCall)node;
            if (nodeDataType == SqlTypeName.BOOLEAN && (!rexCallCanReturnBoolean || call.getOperator().getKind() == SqlKind.CAST)) {
               logger.debug("RexCall returns boolean, {}", node);
               return true;
            }

            List operandsToCheck;
            if (call.getOperator().getKind() != SqlKind.CASE) {
               operandsToCheck = call.getOperands();
            } else {
               assert call.getOperands().size() >= 2;

               List<RexNode> clausesToCheck = (List)call.getOperands().stream().filter((operandx) -> {
                  return operandx.getType() != node.getType();
               }).collect(Collectors.toList());
               Iterator var7 = clausesToCheck.iterator();

               while(var7.hasNext()) {
                  RexNode clause = (RexNode)var7.next();
                  if (this.hasBooleanLiteralOrInputRef(clause)) {
                     return true;
                  }
               }

               rexCallCanReturnBoolean = this.supportsLiteral(CompleteType.BIT);
               operandsToCheck = (List)call.getOperands().stream().filter((operandx) -> {
                  return operandx.getType() == node.getType();
               }).collect(Collectors.toList());
            }

            Iterator var10 = operandsToCheck.iterator();

            while(var10.hasNext()) {
               RexNode operand = (RexNode)var10.next();
               if (this.hasBooleanLiteralOrRexCallReturnsBoolean(operand, rexCallCanReturnBoolean)) {
                  logger.debug("RexCall has boolean inputs, {}", node);
                  return true;
               }
            }
         }

         return false;
      }
   }

   private boolean hasBooleanLiteralOrInputRef(RexNode node) {
      if (!(node instanceof RexLiteral) && !(node instanceof RexInputRef)) {
         if (node instanceof RexCall) {
            Iterator var4 = ((RexCall)node).getOperands().iterator();

            while(var4.hasNext()) {
               RexNode operand = (RexNode)var4.next();
               if (this.hasBooleanLiteralOrInputRef(operand)) {
                  return true;
               }
            }
         }

         return false;
      } else {
         SqlTypeName nodeDataType = node.getType().getSqlTypeName();
         boolean toReturn = nodeDataType == SqlTypeName.BOOLEAN;
         if (toReturn) {
            logger.debug("Boolean {} found, {}", node instanceof RexLiteral ? "RexLiteral" : "RexInputRef", node);
         }

         return toReturn;
      }
   }

   public SqlNode decorateSqlNode(RexNode rexNode, Supplier<SqlNode> defaultNodeSupplier) {
      SqlNode undecoratedNode = (SqlNode)defaultNodeSupplier.get();
      if (undecoratedNode instanceof SqlCall && !MANDATORY_OPERATORS.contains(undecoratedNode.getKind())) {
         Preconditions.checkArgument(rexNode instanceof RexCall);
         RexCall rexCall = (RexCall)rexNode;
         CallTransformer transformer = this.getCallTransformer(rexCall);
         return this.yaml.getSqlNodeForOperator((SqlCall)undecoratedNode, rexCall, transformer);
      } else {
         return (SqlNode)defaultNodeSupplier.get();
      }
   }

   public SqlNode decorateSqlNode(AggregateCall aggCall, Supplier<List<RelDataType>> argTypes, Supplier<SqlNode> defaultNodeSupplier) {
      return this.yaml.getSqlNodeForOperator((SqlCall)defaultNodeSupplier.get(), aggCall, (List)argTypes.get());
   }

   protected final SqlNode emulateNullDirectionWithCaseIsNull(SqlNode node, boolean nullsFirst, boolean desc) {
      if (this.nullCollation.isDefaultOrder(nullsFirst, desc)) {
         return null;
      } else {
         SqlNode orderingNode = new SqlCase(SqlParserPos.ZERO, (SqlNode)null, SqlNodeList.of(SqlStdOperatorTable.IS_NULL.createCall(SqlParserPos.ZERO, new SqlNode[]{node})), SqlNodeList.of(SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)), SqlNodeList.of(SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO)));
         return (SqlNode)(!nullsFirst ? orderingNode : SqlStdOperatorTable.DESC.createCall(SqlParserPos.ZERO, new SqlNode[]{orderingNode}));
      }
   }

   static {
      MANDATORY_OPERATORS = (new Builder()).add(new SqlKind[]{SqlKind.SELECT, SqlKind.AS, SqlKind.CASE}).build();
      logger = LoggerFactory.getLogger(ArpDialect.class);
   }

   private static final class ArpCallTransformers {
      private static final ImmutableMap<SqlOperator, CallTransformer> transformers;

      static CallTransformer getTransformer(RexCall call) {
         CallTransformer transformer = (CallTransformer)transformers.get(call.getOperator());
         return (CallTransformer)(transformer != null && transformer.matches(call) ? transformer : NoOpTransformer.INSTANCE);
      }

      static CallTransformer getTransformer(SqlOperator operator) {
         CallTransformer transformer = (CallTransformer)transformers.get(operator);
         return (CallTransformer)(transformer != null ? transformer : NoOpTransformer.INSTANCE);
      }

      private static void registerTransformer(CallTransformer transformer, com.google.common.collect.ImmutableMap.Builder<SqlOperator, CallTransformer> builder) {
         Iterator var2 = transformer.getCompatibleOperators().iterator();

         while(var2.hasNext()) {
            SqlOperator op = (SqlOperator)var2.next();
            builder.put(op, transformer);
         }

      }

      static {
         com.google.common.collect.ImmutableMap.Builder<SqlOperator, CallTransformer> builder = ImmutableMap.builder();
         registerTransformer(TrimTransformer.INSTANCE, builder);
         registerTransformer(TimeUnitFunctionTransformer.INSTANCE, builder);
         transformers = builder.build();
      }
   }

   @VisibleForTesting
   public static class ArpSchemaFetcher extends JdbcSchemaFetcherImpl {
      private static final Logger logger = LoggerFactory.getLogger(ArpDialect.ArpSchemaFetcher.class);
      private final String query;
      private final boolean usePrepareForColumnMeta;
      private final boolean usePrepareForGetTables;

      @VisibleForTesting
      public String getQuery() {
         return this.query;
      }

      public ArpSchemaFetcher(String query, JdbcPluginConfig config) {
         super(config);
         this.query = query;
         this.usePrepareForColumnMeta = false;
         this.usePrepareForGetTables = false;
      }

      public ArpSchemaFetcher(String query, JdbcPluginConfig config, boolean usePrepareForColumnMeta, boolean usePrepareForGetTables) {
         super(config);
         this.query = query;
         this.usePrepareForColumnMeta = usePrepareForColumnMeta;
         this.usePrepareForGetTables = usePrepareForGetTables;
      }

      public CloseableIterator<CanonicalizeTablePathResponse> listTableNames(ListTableNamesRequest request) {
         logger.debug("Getting all tables for plugin '{}'.", this.getConfig().getSourceName());

         try {
            Connection connection = this.getDataSource().getConnection();
            return new ArpDialect.ArpSchemaFetcher.ArpJdbcTableNamesIterator(this.getConfig().getSourceName(), connection, this.filterQuery(this.query, connection.getMetaData()));
         } catch (SQLException var3) {
            return ArpDialect.ArpSchemaFetcher.EmptyCloseableIterator.getInstance();
         }
      }

      protected boolean usePrepareForColumnMetadata() {
         return this.usePrepareForColumnMeta;
      }

      protected boolean usePrepareForGetTables() {
         return this.usePrepareForGetTables;
      }

      protected String filterQuery(String query, DatabaseMetaData metaData) throws SQLException {
         StringBuilder filterQuery = new StringBuilder(query);
         if (this.getConfig().getDatabase() != null && this.getConfig().showOnlyConnDatabase()) {
            if (supportsCatalogs(this.getConfig().getDialect(), metaData)) {
               filterQuery.append(" AND CAT = ").append(this.getConfig().getDialect().quoteStringLiteral(this.getConfig().getDatabase()));
            } else if (supportsSchemas(this.getConfig().getDialect(), metaData)) {
               filterQuery.append(" AND SCH = ").append(this.getConfig().getDialect().quoteStringLiteral(this.getConfig().getDatabase()));
            }
         }

         return filterQuery.toString();
      }

      private static class EmptyCloseableIterator extends AbstractIterator<CanonicalizeTablePathResponse> implements CloseableIterator<CanonicalizeTablePathResponse> {
         private static ArpDialect.ArpSchemaFetcher.EmptyCloseableIterator singleInstance;

         static ArpDialect.ArpSchemaFetcher.EmptyCloseableIterator getInstance() {
            if (singleInstance == null) {
               singleInstance = new ArpDialect.ArpSchemaFetcher.EmptyCloseableIterator();
            }

            return singleInstance;
         }

         protected CanonicalizeTablePathResponse computeNext() {
            return (CanonicalizeTablePathResponse)this.endOfData();
         }

         public void close() {
         }
      }

      protected static class ArpJdbcTableNamesIterator extends AbstractIterator<CanonicalizeTablePathResponse> implements CloseableIterator<CanonicalizeTablePathResponse> {
         private final String storagePluginName;
         private final Connection connection;
         private Statement statement;
         private ResultSet tablesResult;

         protected ArpJdbcTableNamesIterator(String storagePluginName, Connection connection, String query) {
            this.storagePluginName = storagePluginName;
            this.connection = connection;

            try {
               this.statement = connection.createStatement();
               this.tablesResult = this.statement.executeQuery(query);
            } catch (SQLException var5) {
               ArpDialect.ArpSchemaFetcher.logger.error(String.format("Error retrieving all tables for %s", storagePluginName), var5);
            }

         }

         public CanonicalizeTablePathResponse computeNext() {
            try {
               if (this.tablesResult != null && this.tablesResult.next()) {
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
               } else {
                  ArpDialect.ArpSchemaFetcher.logger.debug("Done fetching all schema and tables for '{}'.", this.storagePluginName);
                  return (CanonicalizeTablePathResponse)this.endOfData();
               }
            } catch (SQLException var4) {
               ArpDialect.ArpSchemaFetcher.logger.error(String.format("Error listing datasets for '%s'", this.storagePluginName), var4);
               return (CanonicalizeTablePathResponse)this.endOfData();
            }
         }

         public void close() throws Exception {
            try {
               AutoCloseables.close(new AutoCloseable[]{this.tablesResult, this.statement, this.connection});
            } catch (Exception var2) {
               ArpDialect.ArpSchemaFetcher.logger.warn("Error closing connection when listing JDBC datasets.", var2);
            }

         }
      }
   }
}
