package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class JoinOp {
   private final boolean enable;
   protected final String rewrite;

   @JsonCreator
   JoinOp(@JsonProperty("enable") boolean enable, @JsonProperty("rewrite") String rewrite) {
      this.enable = enable;
      this.rewrite = rewrite;
   }

   public boolean isEnable() {
      return this.enable;
   }

   public String getRewrite() {
      return this.rewrite;
   }

   public <T> T unwrap(Class<T> iface) {
      return iface.isAssignableFrom(this.getClass()) ? this : null;
   }

   public String toString() {
      return "Is enabled: '" + this.enable + "'\nRewrite: " + this.rewrite + "\n";
   }
}
