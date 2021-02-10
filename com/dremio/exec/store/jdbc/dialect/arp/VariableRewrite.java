package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

class VariableRewrite {
   private final List<String> separatorSequence;
   private final String rewriteArgument;
   private final String rewriteFormat;

   @JsonCreator
   VariableRewrite(@JsonProperty("separator_sequence") List<String> separatorSequence, @JsonProperty("rewrite_argument") String rewriteArgument, @JsonProperty("rewrite_format") String rewriteFormat) {
      this.separatorSequence = separatorSequence;
      this.rewriteArgument = rewriteArgument;
      this.rewriteFormat = rewriteFormat;
   }

   public List<String> getSeparatorSequence() {
      return this.separatorSequence;
   }

   public String getRewriteFormat() {
      return this.rewriteFormat;
   }

   public String getRewriteArgument() {
      return this.rewriteArgument;
   }
}
