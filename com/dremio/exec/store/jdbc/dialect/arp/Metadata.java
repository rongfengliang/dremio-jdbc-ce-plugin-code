package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

class Metadata {
   private final String name;
   private final String apiname;
   private final String version;

   public String getName() {
      return this.name;
   }

   public String getApiname() {
      return this.apiname;
   }

   public String getVersion() {
      return this.version;
   }

   @JsonCreator
   Metadata(@JsonProperty("name") String name, @JsonProperty("apiname") String apiname, @JsonProperty("version") String version) {
      this.name = name;
      this.apiname = apiname;
      this.version = version;
   }
}
