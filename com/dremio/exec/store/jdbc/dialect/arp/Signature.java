package com.dremio.exec.store.jdbc.dialect.arp;

import com.dremio.common.dialect.arp.transformer.CallTransformer;
import com.dremio.common.expression.CompleteType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;

class Signature {
   private final List<CompleteType> args;
   private final CompleteType returnType;

   Signature(String returnType, List<String> args) {
      this.returnType = Mapping.convertDremioTypeStringToCompleteType(returnType);
      this.args = (List)args.stream().map(Mapping::convertDremioTypeStringToCompleteType).collect(Collectors.toList());
   }

   OperatorDescriptor toOperatorDescriptor(String name) {
      return new OperatorDescriptor(name, this.returnType, this.args, false);
   }

   boolean hasRewrite() {
      return false;
   }

   protected List<CompleteType> getArgs() {
      return this.args;
   }

   protected CompleteType getReturnType() {
      return this.returnType;
   }

   @JsonCreator
   static Signature createSignature(@JsonProperty("return") String returnType, @JsonProperty("args") List<String> args, @JsonProperty("rewrite") String rewrite) {
      return (Signature)(rewrite == null ? new Signature(returnType, args) : new RewritingSignature(returnType, args, rewrite));
   }

   public void unparse(SqlCall originalNode, CallTransformer transformer, SqlWriter writer, int leftPrec, int rightPrec) {
      throw new UnsupportedOperationException("Unparse should only be used with rewriting signatures.");
   }
}
