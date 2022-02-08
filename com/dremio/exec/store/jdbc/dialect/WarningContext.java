package com.dremio.exec.store.jdbc.dialect;

class WarningContext {
   private final String key;
   private final String value;

   WarningContext(String key, String value) {
      this.key = key;
      this.value = value;
   }

   public String getKey() {
      return this.key;
   }

   public String getValue() {
      return this.value;
   }
}
