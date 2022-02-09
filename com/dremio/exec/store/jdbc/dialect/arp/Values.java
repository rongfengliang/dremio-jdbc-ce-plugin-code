package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Locale;

public class Values extends RelationalAlgebraOperation {
   @JsonIgnore
   private final Values.Method method;
   @JsonIgnore
   private final String dummyTable;

   @JsonCreator
   public Values(@JsonProperty("enable") boolean enable, @JsonProperty("method") String method, @JsonProperty("dummy_table") String dummyTable) {
      super(enable);
      this.method = Values.Method.valueOf(method.toUpperCase(Locale.ROOT));
      this.dummyTable = dummyTable;
   }

   public Values.Method getMethod() {
      return this.method;
   }

   public String getDummyTable() {
      return this.dummyTable;
   }

   static enum Method {
      VALUES,
      DUMMY_TABLE;
   }
}
