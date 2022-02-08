package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public class Aggregation extends RelationalAlgebraOperation {
   @JsonIgnore
   private final boolean supportsDistinct;
   @JsonIgnore
   private final Map<OperatorDescriptor, Signature> functionMap;
   @JsonIgnore
   private final CountOperations countOperations;

   @JsonCreator
   Aggregation(@JsonProperty("enable") boolean enable, @JsonProperty("count_functions") CountOperations countOperations, @JsonProperty("distinct") boolean supportsDistinct, @JsonProperty("functions") List<OperatorDefinition> operators) {
      super(enable);
      this.supportsDistinct = supportsDistinct;
      this.functionMap = OperatorDefinition.buildOperatorMap(operators);
      this.countOperations = countOperations;
   }

   public boolean supportsDistinct() {
      return this.supportsDistinct;
   }

   public Map<OperatorDescriptor, Signature> getFunctionMap() {
      return this.functionMap;
   }

   CountOperation getCountOperation(CountOperations.CountOperationType operationType) {
      return this.countOperations.getCountOperation(operationType);
   }
}
