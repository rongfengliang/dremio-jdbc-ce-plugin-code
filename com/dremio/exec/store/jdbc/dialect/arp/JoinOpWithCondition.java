package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class JoinOpWithCondition extends JoinOp {
   private final boolean inequality;

   @JsonCreator
   JoinOpWithCondition(@JsonProperty("enable") boolean enable, @JsonProperty("rewrite") String rewrite, @JsonProperty("inequality") boolean inequality) {
      super(enable, rewrite);
      this.inequality = inequality;
   }

   public boolean isInequality() {
      return this.inequality;
   }

   public String toString() {
      return super.toString() + "Supports inequality: " + this.inequality + "\n";
   }
}
