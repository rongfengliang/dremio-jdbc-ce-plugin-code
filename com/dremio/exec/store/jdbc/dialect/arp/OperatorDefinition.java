package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

class OperatorDefinition {
   private final List<String> names;
   private final List<Signature> signatures;

   @JsonCreator
   OperatorDefinition(@JsonProperty("names") List<String> names, @JsonProperty("signatures") List<Signature> signatures) {
      this.names = names;
      this.signatures = signatures;
   }

   List<String> getNames() {
      return this.names;
   }

   List<Signature> getSignatures() {
      return this.signatures;
   }

   public static Map<OperatorDescriptor, Signature> buildOperatorMap(List<? extends OperatorDefinition> operators) {
      Builder<OperatorDescriptor, Signature> builder = ImmutableMap.builder();
      Iterator var2 = operators.iterator();

      Iterator var4;
      Iterator var6;
      while(var2.hasNext()) {
         OperatorDefinition op = (OperatorDefinition)var2.next();
         var4 = op.getNames().iterator();

         while(var4.hasNext()) {
            String name = (String)var4.next();
            var6 = op.getSignatures().iterator();

            while(var6.hasNext()) {
               Signature sig = (Signature)var6.next();
               builder.put(sig.toOperatorDescriptor(name), sig);
            }
         }
      }

      try {
         return builder.build();
      } catch (IllegalArgumentException var11) {
         Set<OperatorDescriptor> debuggingSet = Sets.newHashSet();
         var4 = operators.iterator();

         while(var4.hasNext()) {
            OperatorDefinition op = (OperatorDefinition)var4.next();
            var6 = op.getNames().iterator();

            while(var6.hasNext()) {
               String name = (String)var6.next();
               Iterator var8 = op.getSignatures().iterator();

               while(var8.hasNext()) {
                  Signature sig = (Signature)var8.next();
                  OperatorDescriptor desc = sig.toOperatorDescriptor(name);
                  if (!debuggingSet.add(desc)) {
                     throw new IllegalArgumentException(String.format("Duplicate operator definition: %s", desc), var11);
                  }
               }
            }
         }

         throw var11;
      }
   }
}
