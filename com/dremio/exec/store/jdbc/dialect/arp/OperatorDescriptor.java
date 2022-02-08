package com.dremio.exec.store.jdbc.dialect.arp;

import com.dremio.common.dialect.arp.transformer.CallTransformer;
import com.dremio.common.expression.CompleteType;
import com.dremio.exec.store.jdbc.dialect.SourceTypeDescriptor;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperatorDescriptor {
   private static final Logger logger = LoggerFactory.getLogger(OperatorDescriptor.class);
   private static final CompleteType SIMPLE_DECIMAL = CompleteType.fromDecimalPrecisionScale(0, 0);
   private final String name;
   private final CompleteType returnType;
   private final List<CompleteType> arguments;
   private final boolean isVarArgs;
   private static final Map<String, String> operatorNameToArpNameMap;

   OperatorDescriptor(String name, CompleteType returnType, List<CompleteType> args, boolean isVarArgs) {
      this.name = name;
      this.returnType = returnType;
      this.arguments = args;
      this.isVarArgs = isVarArgs;
   }

   public boolean equals(Object o) {
      if (!(o instanceof OperatorDescriptor)) {
         return false;
      } else {
         OperatorDescriptor rhs = (OperatorDescriptor)o;
         if (Objects.equals(rhs.name.toUpperCase(Locale.ROOT), this.name.toUpperCase(Locale.ROOT)) && Objects.equals(rhs.returnType, this.returnType)) {
            if (!this.isVarArgs) {
               return Objects.equals(rhs.arguments, this.arguments);
            } else {
               return !rhs.arguments.isEmpty() && !this.arguments.isEmpty() ? ((CompleteType)rhs.arguments.get(0)).equals(this.arguments.get(0)) : true;
            }
         } else {
            return false;
         }
      }
   }

   public int hashCode() {
      return !this.isVarArgs ? Objects.hash(new Object[]{this.name.toUpperCase(Locale.ROOT), this.returnType, this.arguments}) : Objects.hash(new Object[]{this.name.toUpperCase(Locale.ROOT), this.returnType});
   }

   public String getName() {
      return this.name;
   }

   public CompleteType getReturnType() {
      return this.returnType;
   }

   public List<CompleteType> getArguments() {
      return this.arguments;
   }

   public boolean isVarArgs() {
      return this.isVarArgs;
   }

   static OperatorDescriptor createFromRexCall(RexCall call, CallTransformer transformer, boolean isDistinct, boolean isVarags) {
      List<RelDataType> argTypes = (List)transformer.transformRexOperands(call.operands).stream().map(RexNode::getType).collect(Collectors.toList());
      RelDataType returnType = call.getType();
      String name = getArpOperatorNameFromOperator(call.getOperator().getName());
      name = transformer.adjustNameBasedOnOperands(name, call.operands);
      logger.debug("Searching in ARP for {} with types {} and return {}", new Object[]{name, argTypes, returnType});
      return createFromRelTypes(name, isDistinct, returnType, argTypes, isVarags);
   }

   static OperatorDescriptor createFromRelTypes(String name, boolean isDistinct, RelDataType returnType, List<RelDataType> argTypes, boolean isVarArgs) {
      CompleteType returnAsCompleteType = SourceTypeDescriptor.getType(returnType);
      if (returnAsCompleteType.isDecimal()) {
         returnAsCompleteType = SIMPLE_DECIMAL;
      }

      List<CompleteType> argsAsCompleteTypes = (List)argTypes.stream().map(SourceTypeDescriptor::getType).map((t) -> {
         return t.isDecimal() ? SIMPLE_DECIMAL : t;
      }).collect(Collectors.toList());
      if (isDistinct) {
         name = name + "_distinct";
      }

      return new OperatorDescriptor(name, returnAsCompleteType, argsAsCompleteTypes, isVarArgs);
   }

   public String toString() {
      StringBuilder sb = (new StringBuilder()).append("Operator name: '").append(this.name).append("'\n").append("Is varargs: ").append(this.isVarArgs).append("\n").append("Argument types: ").append((String)this.arguments.stream().map(CompleteType::toString).collect(Collectors.joining(", "))).append("\n");
      if (this.returnType != null) {
         sb.append("Return type: ").append(this.returnType).append("\n");
      }

      return sb.toString();
   }

   static String getArpOperatorNameFromOperator(String operatorName) {
      String mappedName = (String)operatorNameToArpNameMap.get(operatorName);
      return mappedName != null ? mappedName : operatorName;
   }

   static {
      Builder<String, String> builder = ImmutableMap.builder();
      operatorNameToArpNameMap = builder.put("DATETIME_PLUS", "+").put("DATETIME_MINUS", "-").put("$SUM0", "SUM").build();
   }
}
