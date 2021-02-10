package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CountOperation {
   private final boolean enable;
   private final VarArgsRewritingSignature signature;

   @JsonCreator
   CountOperation(@JsonProperty("enable") boolean enable, @JsonProperty("variable_rewrite") VariableRewrite rewrite) {
      this.enable = enable;
      if (rewrite != null) {
         this.signature = new VarArgsRewritingSignature("bigint", (String)null, rewrite);
      } else {
         this.signature = null;
      }

   }

   public boolean isEnable() {
      return this.enable;
   }

   public VarArgsRewritingSignature getSignature() {
      return this.signature;
   }
}
