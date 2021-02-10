package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SubQuerySupport {
   private final boolean supportsSubQuery;
   private final boolean supportsCorrelated;
   private final boolean supportsScalar;
   private final boolean supportsInClause;

   @JsonCreator
   SubQuerySupport(@JsonProperty("enable") boolean enable, @JsonProperty("correlated") boolean correlated, @JsonProperty("scalar") boolean scalar, @JsonProperty("in_clause") boolean inClause) {
      this.supportsSubQuery = enable;
      this.supportsCorrelated = correlated;
      this.supportsScalar = scalar;
      this.supportsInClause = inClause;
   }

   public boolean isEnabled() {
      return this.supportsSubQuery;
   }

   public boolean getCorrelatedSubQuerySupport() {
      return this.supportsCorrelated;
   }

   public boolean getScalarSupport() {
      return this.supportsScalar;
   }

   public boolean getInClauseSupport() {
      return this.supportsInClause;
   }
}
