package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CountOperations {
   private final CountOperation countStar;
   private final CountOperation countOperation;
   private final CountOperation countMultiOperation;
   private final CountOperation countDistinctOperation;
   private final CountOperation countDistinctMultiOperation;

   @JsonCreator
   CountOperations(@JsonProperty("count_star") CountOperation countStar, @JsonProperty("count") CountOperation count, @JsonProperty("count_multi") CountOperation countMulti, @JsonProperty("count_distinct") CountOperation countDistinct, @JsonProperty("count_distinct_multi") CountOperation countDistinctMulti) {
      this.countOperation = count;
      this.countMultiOperation = countMulti;
      this.countDistinctOperation = countDistinct;
      this.countDistinctMultiOperation = countDistinctMulti;
      this.countStar = countStar;
   }

   public CountOperation getCountOperation(CountOperations.CountOperationType type) {
      switch(type) {
      case COUNT_STAR:
         return this.countStar;
      case COUNT_DISTINCT:
         return this.countDistinctOperation;
      case COUNT_DISTINCT_MULTI:
         return this.countDistinctMultiOperation;
      case COUNT_MULTI:
         return this.countMultiOperation;
      case COUNT:
      default:
         return this.countOperation;
      }
   }

   static enum CountOperationType {
      COUNT_STAR,
      COUNT,
      COUNT_MULTI,
      COUNT_DISTINCT,
      COUNT_DISTINCT_MULTI;
   }
}
