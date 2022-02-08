package com.dremio.exec.store.jdbc.dialect.arp;

import com.dremio.common.dialect.arp.transformer.CallTransformer;
import com.dremio.common.dialect.arp.transformer.NoOpTransformer;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.store.jdbc.dialect.SourceTypeDescriptor;
import com.dremio.exec.work.foreman.UnsupportedDataTypeException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ArpYaml {
   private static final Logger logger = LoggerFactory.getLogger(ArpYaml.class);
   private final Metadata metadata;
   private final Syntax syntax;
   private final DataTypes dataTypes;
   private final RelationalAlgebraOperations relationalAlgebra;
   private final Expressions expressions;

   public static ArpYaml createFromFile(String arpFile) throws IOException {
      ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      URL url = Resources.getResource(arpFile);
      String content = Resources.toString(url, StandardCharsets.UTF_8);
      return (ArpYaml)mapper.readValue(content, ArpYaml.class);
   }

   @JsonCreator
   private ArpYaml(@JsonProperty("metadata") Metadata meta, @JsonProperty("syntax") Syntax syntax, @JsonProperty("data_types") DataTypes dataTypes, @JsonProperty("relational_algebra") RelationalAlgebraOperations algebra, @JsonProperty("expressions") Expressions expressions) {
      this.metadata = meta;
      this.syntax = syntax;
      this.dataTypes = dataTypes;
      this.relationalAlgebra = algebra;
      this.expressions = expressions;
   }

   public Metadata getMetadata() {
      return this.metadata;
   }

   public Syntax getSyntax() {
      return this.syntax;
   }

   public boolean supportsLiteral(CompleteType type) {
      if (type.isDecimal()) {
         Mapping mapping = (Mapping)this.dataTypes.getDefaultCastSpecMap().get(type.getSqlTypeName());
         if (null == mapping) {
            return false;
         } else {
            return (null == mapping.getSource().getMaxScale() || type.getScale() <= mapping.getSource().getMaxScale()) && (null == mapping.getSource().getMaxPrecision() || type.getPrecision() <= mapping.getSource().getMaxPrecision());
         }
      } else {
         return this.dataTypes.getDefaultCastSpecMap().containsKey(type.getSqlTypeName());
      }
   }

   public Mapping getMapping(SourceTypeDescriptor sourceType) throws UnsupportedDataTypeException {
      String dataSourceName = sourceType.getDataSourceTypeName();
      Mapping mapping = (Mapping)this.dataTypes.getSourceTypeToMappingMap().get(dataSourceName);
      if (null == mapping) {
         int index = dataSourceName.lastIndexOf(40);
         if (-1 != index && index < dataSourceName.lastIndexOf(41)) {
            return (Mapping)this.dataTypes.getSourceTypeToMappingMap().get(dataSourceName.substring(0, index).trim());
         }
      }

      return mapping;
   }

   public SqlNode getCastSpec(RelDataType type) {
      CompleteType completeType = SourceTypeDescriptor.getType(type);
      Mapping mapping = (Mapping)this.dataTypes.getDefaultCastSpecMap().get(completeType.getSqlTypeName());
      if (mapping == null) {
         logger.debug("No cast spec found for type: '{}'", type);
         return null;
      } else {
         int precision = type.getPrecision();
         if (completeType.isDecimal()) {
            if (this.isInvalidDecimal(completeType, mapping)) {
               return null;
            }

            precision = 38;
         }

         return new SourceTypeSpec(mapping.getSource().getName().toUpperCase(Locale.ROOT), mapping.getRequiredCastArgs(), precision, type.getScale());
      }
   }

   public SqlNode getSqlNodeForOperator(SqlCall sqlCall, RexCall rexCall, CallTransformer transformer) {
      if (logger.isDebugEnabled()) {
         logger.debug("Searching for scalar operator {} or windowed aggregate for SQL generation.", rexCall);
      }

      Signature sig;
      OperatorDescriptor op;
      if (sqlCall.isA(SqlKind.AGGREGATE)) {
         if (sqlCall.getKind() == SqlKind.COUNT) {
            return this.getNodeForCountOperation(sqlCall);
         }

         op = OperatorDescriptor.createFromRexCall(rexCall, transformer, false, false);
         logger.debug("Searching in aggregation/functions YAML section for operator {}", op);
         sig = (Signature)this.relationalAlgebra.getAggregation().getFunctionMap().get(op);
      } else {
         if (sqlCall.getKind() == SqlKind.OTHER_FUNCTION && 2 == sqlCall.getOperandList().size()) {
            String formatStr;
            String transformedFormatStr;
            SqlNode operand;
            if ("TO_DATE".equalsIgnoreCase(sqlCall.getOperator().getName())) {
               operand = (SqlNode)sqlCall.getOperandList().get(1);
               if (operand.getKind().equals(SqlKind.LITERAL)) {
                  formatStr = (String)((SqlLiteral)operand).getValueAs(String.class);
                  if (null != formatStr) {
                     transformedFormatStr = this.transformDateTimeFormatString(formatStr);
                     sqlCall.setOperand(1, SqlLiteral.createCharString(transformedFormatStr, SqlParserPos.ZERO));
                  }
               }
            } else if ("TO_CHAR".equalsIgnoreCase(sqlCall.getOperator().getName()) && SqlTypeUtil.isNumeric(((RexNode)rexCall.getOperands().get(0)).getType())) {
               operand = (SqlNode)sqlCall.getOperandList().get(1);
               if (operand.getKind().equals(SqlKind.LITERAL)) {
                  formatStr = (String)((SqlLiteral)operand).getValueAs(String.class);
                  if (null != formatStr) {
                     transformedFormatStr = this.transformNumericFormatString(formatStr);
                     sqlCall.setOperand(1, SqlLiteral.createCharString(transformedFormatStr, SqlParserPos.ZERO));
                  }
               }
            }
         }

         op = OperatorDescriptor.createFromRexCall(rexCall, transformer, false, false);
         logger.debug("Searching in operators YAML section for operator {}", op);
         sig = (Signature)this.expressions.getOperators().get(op);
         if (sig == null) {
            op = OperatorDescriptor.createFromRexCall(rexCall, transformer, false, true);
            logger.debug("Searching in variable_length_operators YAML section for operator {}", op);
            sig = (Signature)this.expressions.getVariableOperators().get(OperatorDescriptor.createFromRexCall(rexCall, transformer, false, true));
         }
      }

      if (sig != null && sig.hasRewrite()) {
         logger.debug("Applying rewrite during unparsing: {}", sig);
         return new ArpSqlCall(sig, sqlCall, transformer);
      } else {
         logger.debug("No rewriting signature found. Returning default unparsing syntax.");
         return sqlCall;
      }
   }

   public SqlNode getSqlNodeForOperator(SqlCall undecoratedNode, AggregateCall aggCall, List<RelDataType> types) {
      if (logger.isDebugEnabled()) {
         logger.debug("Searching for aggregate function {} with params {} for SQL generation.", aggCall.getName(), types);
      }

      if (undecoratedNode.getOperator().getKind() == SqlKind.COUNT) {
         return this.getNodeForCountOperation(undecoratedNode);
      } else {
         Signature sig = (Signature)this.relationalAlgebra.getAggregation().getFunctionMap().get(OperatorDescriptor.createFromRelTypes(OperatorDescriptor.getArpOperatorNameFromOperator(aggCall.getAggregation().getName()), aggCall.isDistinct(), aggCall.getType(), types, false));
         if (sig != null && sig.hasRewrite()) {
            logger.debug("Applying rewrite during unparsing: {}", sig);
            return new ArpSqlCall(sig, undecoratedNode, NoOpTransformer.INSTANCE);
         } else {
            logger.debug("No rewriting signature found. Returning default unparsing syntax.");
            return undecoratedNode;
         }
      }
   }

   public boolean supportsScalarOperator(SqlOperator operator, List<RelDataType> argTypes, RelDataType returnType) {
      logger.debug("Identifying if operator {} is supported.", operator.getName());
      OperatorDescriptor op = OperatorDescriptor.createFromRelTypes(OperatorDescriptor.getArpOperatorNameFromOperator(operator.getName()), false, returnType, argTypes, false);
      logger.debug("Searching for operator {} in expressions/operators section in YAML.", op);
      boolean supportsOperator = this.expressions.getOperators().containsKey(op);
      if (!supportsOperator) {
         logger.debug("Operator not found in expressions/operators section.");
         op = OperatorDescriptor.createFromRelTypes(OperatorDescriptor.getArpOperatorNameFromOperator(operator.getName()), false, returnType, argTypes, true);
         logger.debug("Searching for operator {} in variable_length_operators section in YAML.", op);
         supportsOperator = this.expressions.getVariableOperators().containsKey(op);
      }

      if (!supportsOperator) {
         logger.debug("Operator {} not supported. Aborting pushdown.", operator.getName());
      } else {
         logger.debug("Operator {} supported.", operator.getName());
      }

      return supportsOperator;
   }

   public boolean supportsAggregate(SqlOperator operator, boolean isDistinct, List<RelDataType> argTypes, RelDataType returnType) {
      logger.debug("Checking if aggregation is enabled.");
      if (!this.relationalAlgebra.getAggregation().isEnabled()) {
         return false;
      } else {
         OperatorDescriptor op = OperatorDescriptor.createFromRelTypes(OperatorDescriptor.getArpOperatorNameFromOperator(operator.getName()), isDistinct, returnType, argTypes, false);
         logger.debug("Searching aggregation/functions in YAML for aggregate {}", op);
         boolean supportsAggregate = this.relationalAlgebra.getAggregation().getFunctionMap().containsKey(op);
         if (!supportsAggregate) {
            logger.debug("Aggregate {} not supported. Aborting pushdown.", op);
         } else {
            logger.debug("Aggregate {} supported.", op);
         }

         return supportsAggregate;
      }
   }

   public boolean supportsTimeUnitFunction(SqlOperator operator, TimeUnitRange unit, List<RelDataType> argTypes, RelDataType returnType) {
      Preconditions.checkArgument(argTypes.size() > 1, String.format("At least one argument other than time unit is expected for %s.", OperatorDescriptor.getArpOperatorNameFromOperator(operator.getName())));
      OperatorDescriptor op = OperatorDescriptor.createFromRelTypes(OperatorDescriptor.getArpOperatorNameFromOperator(operator.getName()) + "_" + unit.toString(), false, returnType, argTypes.subList(1, argTypes.size()), false);
      logger.debug("Checking expressions/operators the time unit-based function {} is supported.", op);
      boolean isSupported = this.expressions.getOperators().containsKey(op);
      if (!isSupported) {
         logger.debug("Time unit {} not supported. Aborting pushdown.", op);
      } else {
         logger.debug("Time unit function {} supported.", op);
      }

      return isSupported;
   }

   public boolean supportsAggregation() {
      return this.relationalAlgebra.getAggregation().isEnabled();
   }

   public boolean supportsDistinct() {
      return this.relationalAlgebra.getAggregation().supportsDistinct();
   }

   public boolean supportsCountOperation(AggregateCall call) {
      Preconditions.checkArgument(call.getAggregation().getKind() == SqlKind.COUNT);
      logger.debug("Checking if count operation is supported.");
      CountOperations.CountOperationType opType;
      if (call.isDistinct()) {
         if (call.getArgList().size() > 1) {
            opType = CountOperations.CountOperationType.COUNT_DISTINCT_MULTI;
         } else {
            opType = CountOperations.CountOperationType.COUNT_DISTINCT;
         }
      } else if (call.getArgList().isEmpty()) {
         opType = CountOperations.CountOperationType.COUNT_STAR;
      } else if (call.getArgList().size() > 1) {
         opType = CountOperations.CountOperationType.COUNT_MULTI;
      } else {
         opType = CountOperations.CountOperationType.COUNT;
      }

      logger.debug("Searching count_functions section in YAML for {}.", opType);
      if (this.relationalAlgebra.getAggregation().getCountOperation(opType).isEnable()) {
         logger.debug("Count function supported.");
         return true;
      } else {
         logger.debug("Count function not supported. Aborting pushdown.");
         return false;
      }
   }

   public boolean supportsCountOperation(SqlOperator op, List<RelDataType> args) {
      Preconditions.checkArgument(op.getKind() == SqlKind.COUNT);
      logger.debug("Checking if count operation is supported.");
      boolean isSupported;
      if (args.isEmpty()) {
         logger.debug("Searching count_functions section in YAML for count_star.");
         isSupported = this.relationalAlgebra.getAggregation().getCountOperation(CountOperations.CountOperationType.COUNT_STAR).isEnable();
      } else {
         logger.debug("Searching count_functions section in YAML for count.");
         isSupported = this.relationalAlgebra.getAggregation().getCountOperation(CountOperations.CountOperationType.COUNT).isEnable();
      }

      if (isSupported) {
         logger.debug("Count function supported.");
      } else {
         logger.debug("Count function not supported. Aborting pushdown.");
      }

      return isSupported;
   }

   private SqlNode getNodeForCountOperation(SqlCall call) {
      Preconditions.checkArgument(call.getOperator() == SqlStdOperatorTable.COUNT);
      logger.debug("Checking if count operation has a variable_rewrite.");
      CountOperations.CountOperationType opType;
      if (call.getFunctionQuantifier() != null && call.getFunctionQuantifier().getValue().equals(SqlSelectKeyword.DISTINCT)) {
         if (call.operandCount() > 1) {
            opType = CountOperations.CountOperationType.COUNT_DISTINCT_MULTI;
         } else {
            opType = CountOperations.CountOperationType.COUNT_DISTINCT;
         }
      } else if (call.operandCount() == 0) {
         opType = CountOperations.CountOperationType.COUNT_STAR;
      } else if (call.operandCount() > 1) {
         opType = CountOperations.CountOperationType.COUNT_MULTI;
      } else {
         opType = CountOperations.CountOperationType.COUNT;
      }

      logger.debug("Searching count_functions section in YAML for {}.", opType);
      Signature sig = this.relationalAlgebra.getAggregation().getCountOperation(opType).getSignature();
      if (sig == null) {
         logger.debug("Count rewrite not needed.");
         return call;
      } else {
         logger.debug("Count variable rewrite: {}", sig);
         return new ArpSqlCall(sig, call, NoOpTransformer.INSTANCE);
      }
   }

   private boolean isInvalidDecimal(CompleteType completeType, Mapping mapping) {
      return null != mapping.getSource().getMaxPrecision() && completeType.getPrecision() > mapping.getSource().getMaxPrecision() || null != mapping.getSource().getMaxScale() && (completeType.getScale() > mapping.getSource().getMaxScale() || completeType.getScale() < 0);
   }

   public String getValuesDummyTable() {
      return this.relationalAlgebra.getValues().getMethod() == Values.Method.DUMMY_TABLE ? this.relationalAlgebra.getValues().getDummyTable() : null;
   }

   public boolean supportsUnion() {
      return this.relationalAlgebra.getUnion().isEnabled();
   }

   public boolean supportsUnionAll() {
      return this.relationalAlgebra.getUnionAll().isEnabled();
   }

   public boolean allowsSortInSetOperand() {
      return this.relationalAlgebra.supportsSortInSetOperator();
   }

   public boolean supportsLimit() {
      return this.relationalAlgebra.getSort().getFetchOffset().getFetchOnly().isEnable();
   }

   public boolean supportsFetchOffset() {
      return this.relationalAlgebra.getSort().getFetchOffset().getOffsetFetch().isEnable();
   }

   public boolean supportsOffset() {
      return this.relationalAlgebra.getSort().getFetchOffset().getOffsetOnly().isEnable();
   }

   public boolean supportsOrderBy() {
      return this.relationalAlgebra.getSort().getOrderBy().isEnabled();
   }

   public NullCollation getNullCollation() {
      switch(this.relationalAlgebra.getSort().getOrderBy().getDefaultNullsOrdering()) {
      case FIRST:
         return NullCollation.FIRST;
      case HIGH:
         return NullCollation.HIGH;
      case LAST:
         return NullCollation.LAST;
      case LOW:
      default:
         return NullCollation.LOW;
      }
   }

   public String getLimitFormat() {
      return this.relationalAlgebra.getSort().getFetchOffset().getFetchOnly().getFormat();
   }

   public String getFetchOffsetFormat() {
      return this.relationalAlgebra.getSort().getFetchOffset().getOffsetFetch().getFormat();
   }

   public String getOffsetFormat() {
      return this.relationalAlgebra.getSort().getFetchOffset().getOffsetOnly().getFormat();
   }

   public JoinOp getJoinSupport(JoinType joinType) {
      JoinOp joinOp = this.relationalAlgebra.getJoin().getJoinOp(joinType);
      logger.debug("Searching join section in YAML for join type '{}', found {}", joinType, joinOp);
      return joinOp;
   }

   public DateTimeFormatSupport getDateTimeFormatSupport() {
      return this.expressions.getDateTimeFormatSupport();
   }

   public NumericFormatSupport getNumericFormatSupport() {
      return this.expressions.getNumericFormatSupport();
   }

   public boolean supportsSubquery() {
      return this.expressions.getSubQuerySupport().isEnabled();
   }

   public boolean supportsCorrelatedSubquery() {
      return this.expressions.getSubQuerySupport().getCorrelatedSubQuerySupport();
   }

   public boolean supportsScalarSubquery() {
      return this.expressions.getSubQuerySupport().getScalarSupport();
   }

   public boolean supportsInClause() {
      return this.expressions.getSubQuerySupport().getInClauseSupport();
   }

   public boolean shouldInjectNumericCastToProject() {
      return this.syntax.shouldInjectNumericCastToProject();
   }

   public boolean shouldInjectApproxNumericCastToProject() {
      return this.syntax.shouldInjectApproxNumericCastToProject();
   }

   public boolean mapBooleanToBitExpr() {
      return this.syntax.mapBooleanToBitExpr();
   }

   private String transformDateTimeFormatString(String dremioDateTimeFormatStr) {
      Iterator var2 = this.getDateTimeFormatSupport().getDateTimeFormatMappings().iterator();

      while(true) {
         DateTimeFormatSupport.DateTimeFormatMapping dtFormat;
         do {
            if (!var2.hasNext()) {
               return dremioDateTimeFormatStr;
            }

            dtFormat = (DateTimeFormatSupport.DateTimeFormatMapping)var2.next();
         } while(!dremioDateTimeFormatStr.contains(dtFormat.getDremioDateTimeFormatString()));

         if (null == dtFormat.getSourceDateTimeFormat() || !dtFormat.getSourceDateTimeFormat().isEnable()) {
            throw new IllegalStateException("Datetime string format '" + dtFormat.getDremioDateTimeFormatString() + "' is not supported.");
         }

         if (!dtFormat.areDateTimeFormatsEqual()) {
            String formatRegex = "((" + dtFormat.getDremioDateTimeFormatString() + ")(?=(?:[^\"]|\"[^\"]*\")*$))+";
            dremioDateTimeFormatStr = dremioDateTimeFormatStr.replaceAll(formatRegex, dtFormat.getSourceDateTimeFormat().getFormat());
         }
      }
   }

   private String transformNumericFormatString(String dremioNumericFormatStr) {
      String escapeQuote = this.getNumericFormatSupport().getEscapeQuote();
      Iterator var3 = this.getNumericFormatSupport().getNumericFormatMappings().iterator();

      while(true) {
         NumericFormatSupport.NumericFormatMapping format;
         do {
            if (!var3.hasNext()) {
               return dremioNumericFormatStr;
            }

            format = (NumericFormatSupport.NumericFormatMapping)var3.next();
         } while(!dremioNumericFormatStr.contains(format.getDremioNumericFormatString()));

         if (null == format.getSourceNumericFormat() || !format.getSourceNumericFormat().isEnable()) {
            throw new IllegalStateException("Numeric string format '" + format.getDremioNumericFormatString() + "' is not supported.");
         }

         if (!format.areNumericFormatsEqual()) {
            String formatRegex = "((" + format.getDremioNumericFormatString() + ")(?=(?:[^\\Q" + escapeQuote + "\\E]|\"[^\\Q" + escapeQuote + "\\E]*\\Q" + escapeQuote + "\\E)*$))";
            dremioNumericFormatStr = dremioNumericFormatStr.replaceAll(formatRegex, format.getSourceNumericFormat().getFormat());
         }
      }
   }
}
