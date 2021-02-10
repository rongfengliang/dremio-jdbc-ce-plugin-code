package com.dremio.exec.store.jdbc.rules;

import com.dremio.common.dialect.arp.transformer.CallTransformer;
import com.dremio.common.dialect.arp.transformer.NoOpTransformer;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.store.jdbc.EnumParameterUtils;
import com.dremio.exec.store.jdbc.conf.DialectConf;
import com.dremio.exec.store.jdbc.dialect.SourceTypeDescriptor;
import com.dremio.exec.store.jdbc.dialect.arp.transformer.TimeUnitFunctionTransformer;
import com.dremio.exec.store.jdbc.legacy.JdbcDremioSqlDialect;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcExpressionSupportCheck implements RexVisitor<Boolean> {
   private static final Logger logger = LoggerFactory.getLogger(JdbcExpressionSupportCheck.class);
   private final JdbcDremioSqlDialect dialect;
   private final RexBuilder builder;

   public static boolean hasOnlySupportedFunctions(RexNode rex, StoragePluginId pluginId, RexBuilder builder) {
      DialectConf<?, ?> conf = (DialectConf)pluginId.getConnectionConf();
      return (Boolean)rex.accept(new JdbcExpressionSupportCheck(conf.getDialect(), builder));
   }

   private JdbcExpressionSupportCheck(JdbcDremioSqlDialect dialect, RexBuilder builder) {
      this.dialect = dialect;
      this.builder = builder;
   }

   public Boolean visitInputRef(RexInputRef paramRexInputRef) {
      return true;
   }

   public Boolean visitLocalRef(RexLocalRef paramRexLocalRef) {
      return true;
   }

   public Boolean visitCall(RexCall paramRexCall) {
      List<RexNode> operands = paramRexCall.getOperands();
      List<RelDataType> paramTypes = (List)operands.stream().map(RexNode::getType).collect(Collectors.toList());
      logger.debug("Evaluating support for {} with operand types {} and return type {}", new Object[]{paramRexCall.getOperator().getName(), paramTypes, paramRexCall.getType()});
      CallTransformer transformer = this.dialect.getCallTransformer(paramRexCall);
      boolean supportsFunction;
      if (transformer == TimeUnitFunctionTransformer.INSTANCE) {
         logger.debug("Operator {} has been identified as a time unit function. Checking support using supportsTimeUnitFunction().", paramRexCall.getOperator().getName());
         TimeUnitRange timeUnitRange = EnumParameterUtils.getFirstParamAsTimeUnitRange(operands);
         supportsFunction = this.dialect.supportsTimeUnitFunction(paramRexCall.getOperator(), timeUnitRange, paramRexCall.getType(), paramTypes);
      } else {
         SqlOperator operator;
         if (transformer != NoOpTransformer.INSTANCE) {
            operator = transformer.getAlternateOperator(paramRexCall);
            paramTypes = (List)transformer.transformRexOperands(paramRexCall.operands).stream().map(RexNode::getType).collect(Collectors.toList());
         } else {
            operator = paramRexCall.getOperator();
         }

         logger.debug("Verifying support for operator {} using supportsFunction().", paramRexCall.getOperator().getName());
         if (this.dialect.supportsFunction(operator, paramRexCall.getType(), paramTypes)) {
            if ("TO_DATE".equalsIgnoreCase(operator.getName())) {
               supportsFunction = this.supportsDateTimeFormatString(operator, operands, 1);
            } else if (operator.getName().toUpperCase(Locale.ROOT).startsWith("REGEXP_")) {
               supportsFunction = this.supportsRegexString(operator, operands);
            } else {
               supportsFunction = true;
            }
         } else {
            supportsFunction = false;
         }
      }

      if (supportsFunction) {
         int i = 0;
         Iterator var7 = operands.iterator();

         RexNode operand;
         do {
            if (!var7.hasNext()) {
               logger.debug("Operator {} was supported.", paramRexCall.getOperator().getName());
               return true;
            }

            operand = (RexNode)var7.next();
            ++i;
         } while((Boolean)operand.accept(this));

         logger.debug("Operand {} ({}) for operator {} was not supported. Aborting pushdown.", new Object[]{i, operand.toString(), paramRexCall.getOperator().getName()});
         return false;
      } else if (!this.dialect.useTimestampAddInsteadOfDatetimePlus() || paramRexCall.getOperator() != SqlStdOperatorTable.DATETIME_PLUS && paramRexCall.getOperator() != SqlStdOperatorTable.DATETIME_MINUS) {
         logger.debug("Operator {} was not supported. Aborting pushdown of a RelNode using this operator.", paramRexCall.getOperator().getName());
         return false;
      } else {
         logger.debug("Datetime + interval operation is unsupported, but dialect allows fallback to TIMESTAMPADD pushdown.");
         logger.debug("Attempting to pushdown as TIMESTAMPADD.");
         return this.visitDatetimePlusAsTimestampAdd(paramRexCall);
      }
   }

   public Boolean visitLiteral(RexLiteral literal) {
      if (literal.getTypeName().isSpecial()) {
         return true;
      } else {
         logger.debug("Literal of type {} encountered. Calling supportsLiteral().", literal.getType());
         return this.dialect.supportsLiteral(SourceTypeDescriptor.getType(literal.getType()));
      }
   }

   public Boolean visitPatternFieldRef(RexPatternFieldRef fieldRef) {
      return true;
   }

   public Boolean visitTableInputRef(RexTableInputRef fieldRef) {
      return true;
   }

   public Boolean visitOver(RexOver over) {
      if (!this.visitCall(over)) {
         return false;
      } else {
         RexWindow window = over.getWindow();
         UnmodifiableIterator var3 = window.orderKeys.iterator();

         RexFieldCollation orderKey;
         do {
            if (!var3.hasNext()) {
               var3 = window.partitionKeys.iterator();

               RexNode partitionKey;
               do {
                  if (!var3.hasNext()) {
                     return true;
                  }

                  partitionKey = (RexNode)var3.next();
               } while((Boolean)partitionKey.accept(this));

               return false;
            }

            orderKey = (RexFieldCollation)var3.next();
         } while((Boolean)((RexNode)orderKey.left).accept(this));

         return false;
      }
   }

   public Boolean visitCorrelVariable(RexCorrelVariable paramRexCorrelVariable) {
      return true;
   }

   public Boolean visitDynamicParam(RexDynamicParam paramRexDynamicParam) {
      return true;
   }

   public Boolean visitRangeRef(RexRangeRef paramRexRangeRef) {
      return true;
   }

   public Boolean visitFieldAccess(RexFieldAccess paramRexFieldAccess) {
      return (Boolean)paramRexFieldAccess.getReferenceExpr().accept(this);
   }

   public Boolean visitSubQuery(RexSubQuery subQuery) {
      Iterator var2 = subQuery.getOperands().iterator();

      RexNode operand;
      do {
         if (!var2.hasNext()) {
            return true;
         }

         operand = (RexNode)var2.next();
      } while((Boolean)operand.accept(this));

      return false;
   }

   private Boolean visitDatetimePlusAsTimestampAdd(RexCall call) {
      Preconditions.checkState(this.dialect.useTimestampAddInsteadOfDatetimePlus());
      Preconditions.checkArgument(call.getOperator() == SqlStdOperatorTable.DATETIME_MINUS || call.getOperator() == SqlStdOperatorTable.DATETIME_PLUS);
      logger.debug("Attempting to pushdown conversion of DATETIME_PLUS to TIMESTAMPADD.");
      if (!RexUtil.isInterval((RexNode)call.getOperands().get(1))) {
         logger.debug("The operand applied to DATETIME_PLUS was not an interval. Aborting pushdown.");
         return false;
      } else if (!(Boolean)((RexNode)call.getOperands().get(0)).accept(this)) {
         logger.debug("The datetime expression applied to DATETIME_PLUS was not supported. Aborting pushdown.");
         return false;
      } else {
         RexNode interval = (RexNode)call.getOperands().get(1);
         RelDataTypeFactory factory = this.builder.getTypeFactory();
         TimeUnit startUnit = interval.getType().getSqlTypeName().getStartUnit();
         TimeUnit endUnit = interval.getType().getSqlTypeName().getEndUnit();
         TimeUnitRange timeUnit;
         if (startUnit == endUnit) {
            timeUnit = TimeUnitRange.of(startUnit, (TimeUnit)null);
         } else {
            timeUnit = TimeUnitRange.of(startUnit, endUnit);
         }

         Preconditions.checkNotNull(timeUnit, "Time unit must be constructed correctly.");
         logger.debug("Checking if TIMESTAMPADD is supported using supportsTimeUnitFunction()");
         if (!this.dialect.supportsTimeUnitFunction(SqlStdOperatorTable.TIMESTAMP_ADD, timeUnit, call.getType(), ImmutableList.of(factory.createSqlType(SqlTypeName.ANY), factory.createSqlType(SqlTypeName.INTEGER), ((RexNode)call.getOperands().get(0)).getType()))) {
            logger.debug("Failed to pushdown conversion of DATETIME_PLUS to TIMESTAMPADD .");
            return false;
         } else {
            logger.debug("Checking if the interval operand in the DATETIME_PLUS operation is supported.");
            if (interval instanceof RexLiteral) {
               logger.debug("Successfully pushed down conversion of DATETIME_PLUS to TIMESTAMPADD.");
               return true;
            } else {
               if (interval instanceof RexCall && ((RexCall)interval).getOperator() == SqlStdOperatorTable.MULTIPLY) {
                  RexCall multiplyOp = (RexCall)interval;
                  RexNode leftOp = (RexNode)multiplyOp.getOperands().get(0);
                  RexNode rightOp = (RexNode)multiplyOp.getOperands().get(1);
                  if (RexUtil.isIntervalLiteral(leftOp)) {
                     return this.canReplaceIntervalMultiplicationWithIntegerMultiplication(rightOp);
                  }

                  if (RexUtil.isIntervalLiteral(rightOp)) {
                     return this.canReplaceIntervalMultiplicationWithIntegerMultiplication(leftOp);
                  }
               }

               logger.debug("Failed to pushdown conversion of DATETIME_PLUS to TIMESTAMPADD.");
               return false;
            }
         }
      }
   }

   private boolean canReplaceIntervalMultiplicationWithIntegerMultiplication(RexNode coefficient) {
      logger.debug("Checking if Multiply is supported.");
      RelDataType multiplyDataType = this.builder.deriveReturnType(SqlStdOperatorTable.MULTIPLY, ImmutableList.of(coefficient, this.builder.makeExactLiteral(BigDecimal.ONE)));
      if (!this.dialect.supportsFunction(SqlStdOperatorTable.MULTIPLY, multiplyDataType, ImmutableList.of(coefficient.getType(), this.builder.getTypeFactory().createSqlType(SqlTypeName.INTEGER)))) {
         logger.debug("Failed to pushdown conversion of DATETIME_PLUS to TIMESTAMPADD.");
         return false;
      } else {
         boolean supported = (Boolean)coefficient.accept(this);
         if (supported) {
            logger.debug("Successfully pushed down conversion of DATETIME_PLUS to TIMESTAMPADD.");
         } else {
            logger.debug("Failed to pushdown conversion of DATETIME_PLUS to TIMESTAMPADD ");
         }

         return supported;
      }
   }

   private boolean supportsDateTimeFormatString(SqlOperator operator, List<RexNode> operands, int index) {
      String dateFormatStr = this.getOperandAsString(operator, operands, index);
      if (dateFormatStr == null) {
         return false;
      } else {
         logger.debug("Operator {} has been identified as having a datetime format string. Checking support using supportsDateTimeFormatString().", operator.getName());
         return this.dialect.supportsDateTimeFormatString(dateFormatStr);
      }
   }

   private boolean supportsRegexString(SqlOperator operator, List<RexNode> operands) {
      String regex = this.getOperandAsString(operator, operands, 1);
      if (null == regex) {
         return false;
      } else {
         logger.debug("Operator {} has been identified as having a regex string. Checking support using supportsRegexString().", operator.getName());
         return this.dialect.supportsRegexString(regex);
      }
   }

   private String getOperandAsString(SqlOperator operator, List<RexNode> operands, int index) {
      if (operands.size() >= index && operands.get(index) instanceof RexLiteral) {
         RexLiteral literal = (RexLiteral)operands.get(index);
         String dateFormatStr = (String)literal.getValueAs(String.class);
         if (null == dateFormatStr) {
            logger.debug("Operator {} was not supported due to a non-string literal value. Aborting pushdown.", operator.getName());
            return null;
         } else {
            return dateFormatStr;
         }
      } else {
         logger.debug("Operator {} was not supported due to a non-string literal value. Aborting pushdown.", operator.getName());
         return null;
      }
   }
}
