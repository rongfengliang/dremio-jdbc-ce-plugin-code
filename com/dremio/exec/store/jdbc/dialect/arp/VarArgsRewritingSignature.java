package com.dremio.exec.store.jdbc.dialect.arp;

import com.dremio.common.dialect.arp.transformer.CallTransformer;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;

class VarArgsRewritingSignature extends RewritingSignature {
   private final VariableRewrite rewrite;

   @JsonCreator
   VarArgsRewritingSignature(@JsonProperty("return") String returnType, @JsonProperty("arg_type") String argType, @JsonProperty("variable_rewrite") VariableRewrite rewrite) {
      super(returnType, argType != null ? ImmutableList.of(argType) : ImmutableList.of(), rewrite == null ? null : getRewrite(rewrite.getRewriteFormat()));
      this.rewrite = rewrite;
   }

   boolean hasRewrite() {
      return this.rewrite != null;
   }

   protected List<String> getOperatorsAsStringList(SqlCall originalNode, CallTransformer transformer, SqlWriter writer) {
      List<String> operandsAsList = super.getOperatorsAsStringList(originalNode, transformer, writer);
      if (null != this.rewrite.getRewriteArgument()) {
         operandsAsList = (List)operandsAsList.stream().map((operand) -> {
            return MessageFormat.format(getRewrite(this.rewrite.getRewriteArgument()), operand);
         }).collect(Collectors.toList());
      }

      Builder<String> listBuilder = new Builder();
      Iterator var6 = this.rewrite.getSeparatorSequence().iterator();

      while(var6.hasNext()) {
         String separator = (String)var6.next();
         listBuilder.add(Joiner.on(separator).join(operandsAsList));
      }

      return listBuilder.build();
   }

   OperatorDescriptor toOperatorDescriptor(String name) {
      return new OperatorDescriptor(name, this.getReturnType(), this.getArgs(), true);
   }

   private static String getRewrite(String rewrite) {
      return rewrite == null ? null : rewrite.replaceAll("\\{separator\\[([\\d+?])\\]}", "{$1}");
   }
}
