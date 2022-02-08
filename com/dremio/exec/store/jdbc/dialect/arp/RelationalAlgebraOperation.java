package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RelationalAlgebraOperation {
   private final boolean enable;

   @JsonCreator
   RelationalAlgebraOperation(@JsonProperty("enable") boolean enable) {
      this.enable = enable;
   }

   public boolean isEnabled() {
      return this.enable;
   }
}
