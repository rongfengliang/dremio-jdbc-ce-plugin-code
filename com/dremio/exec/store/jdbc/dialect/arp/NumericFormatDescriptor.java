package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class NumericFormatDescriptor {
   private final boolean enable;
   private final String format;

   @JsonCreator
   NumericFormatDescriptor(@JsonProperty("enable") boolean enable, @JsonProperty("format") String format) {
      this.enable = enable;
      this.format = format;
   }

   public boolean isEnable() {
      return this.enable;
   }

   public String getFormat() {
      return this.format;
   }
}
