package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.List;

public class VariableOperatorDefinition extends OperatorDefinition {
   @JsonCreator
   VariableOperatorDefinition(@JsonProperty("names") List<String> names, @JsonProperty("variable_signatures") List<VarArgsRewritingSignature> signatures) {
      super(names, ImmutableList.copyOf(signatures));
   }
}
