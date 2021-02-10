package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DataType {
   private final String name;
   private final Integer maxScale;
   private final Integer maxPrecision;

   @JsonCreator
   DataType(@JsonProperty("name") String name, @JsonProperty("max_scale") Integer maxScale, @JsonProperty("max_precision") Integer maxPrecision) {
      this.name = name;
      this.maxScale = maxScale;
      this.maxPrecision = maxPrecision;
   }

   public String getName() {
      return this.name;
   }

   public Integer getMaxScale() {
      return this.maxScale;
   }

   public Integer getMaxPrecision() {
      return this.maxPrecision;
   }
}
