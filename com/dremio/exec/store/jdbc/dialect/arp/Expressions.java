package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

class Expressions {
   private final Map<OperatorDescriptor, Signature> operators;
   private final Map<OperatorDescriptor, Signature> variableOperators;
   private final SubQuerySupport subQuerySupport;
   private final DateTimeFormatSupport dateTimeFormatSupport;

   @JsonCreator
   Expressions(@JsonProperty("operators") List<OperatorDefinition> operators, @JsonProperty("variable_length_operators") List<VariableOperatorDefinition> variableOperators, @JsonProperty("subqueries") SubQuerySupport subQuerySupport, @JsonProperty("datetime_formats") DateTimeFormatSupport dateTimeFormatSupport) {
      this.operators = OperatorDefinition.buildOperatorMap(operators);
      this.variableOperators = VariableOperatorDefinition.buildOperatorMap(variableOperators);
      this.subQuerySupport = subQuerySupport;
      this.dateTimeFormatSupport = dateTimeFormatSupport;
   }

   DateTimeFormatSupport getDateTimeFormatSupport() {
      return this.dateTimeFormatSupport;
   }

   Map<OperatorDescriptor, Signature> getOperators() {
      return this.operators;
   }

   Map<OperatorDescriptor, Signature> getVariableOperators() {
      return this.variableOperators;
   }

   SubQuerySupport getSubQuerySupport() {
      return this.subQuerySupport;
   }
}
