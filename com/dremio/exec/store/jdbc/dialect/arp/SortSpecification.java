package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SortSpecification extends RelationalAlgebraOperation {
   private final FetchOffset fetchOffset;
   private final OrderBy orderBy;

   @JsonCreator
   SortSpecification(@JsonProperty("enable") boolean enable, @JsonProperty("fetch_offset") FetchOffset fetchOffset, @JsonProperty("order_by") OrderBy orderBy) {
      super(enable);
      this.fetchOffset = fetchOffset;
      this.orderBy = orderBy;
   }

   public FetchOffset getFetchOffset() {
      return this.fetchOffset;
   }

   public OrderBy getOrderBy() {
      return this.orderBy;
   }
}
