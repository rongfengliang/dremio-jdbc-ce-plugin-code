package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Locale;

public class OrderBy {
   private final boolean enable;
   private final OrderBy.Ordering defaultNullsOrdering;

   @JsonCreator
   OrderBy(@JsonProperty("enable") boolean enable, @JsonProperty("default_nulls_ordering") String ordering) {
      this.enable = enable;
      this.defaultNullsOrdering = OrderBy.Ordering.valueOf(ordering.toUpperCase(Locale.ROOT));
   }

   public boolean isEnabled() {
      return this.enable;
   }

   public OrderBy.Ordering getDefaultNullsOrdering() {
      return this.defaultNullsOrdering;
   }

   static enum Ordering {
      FIRST,
      HIGH,
      LAST,
      LOW;
   }
}
