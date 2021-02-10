package com.dremio.exec.store.jdbc.dialect.arp;

import com.dremio.common.map.CaseInsensitiveMap;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

class DataTypes {
   @JsonIgnore
   private final Map<String, Mapping> defaultCastSpecMap;
   @JsonIgnore
   private final Map<String, Mapping> sourceTypeToMappingMap;

   @JsonCreator
   DataTypes(@JsonProperty("mappings") List<Mapping> mappings) {
      Map<String, Mapping> interimCastMap = Maps.newHashMap();
      Iterator var3 = mappings.iterator();

      while(true) {
         Mapping mapping;
         do {
            if (!var3.hasNext()) {
               this.defaultCastSpecMap = CaseInsensitiveMap.newImmutableMap(interimCastMap);
               Map<String, Mapping> interimSourceMap = Maps.newHashMap();
               Iterator var8 = mappings.iterator();

               Mapping mapping;
               Mapping oldMapping;
               do {
                  if (!var8.hasNext()) {
                     this.sourceTypeToMappingMap = CaseInsensitiveMap.newImmutableMap(interimSourceMap);
                     return;
                  }

                  mapping = (Mapping)var8.next();
                  oldMapping = (Mapping)interimSourceMap.put(mapping.getSource().getName().toUpperCase(Locale.ROOT), mapping);
               } while(oldMapping == null);

               throw new IllegalArgumentException(String.format("Duplicate mapping found for source type %s:", mapping.getSource().getName()));
            }

            mapping = (Mapping)var3.next();
         } while(!mapping.isDefaultCastSpec() && interimCastMap.containsKey(mapping.getDremio().getSqlTypeName()));

         interimCastMap.put(mapping.getDremio().getSqlTypeName(), mapping);
      }
   }

   public Map<String, Mapping> getDefaultCastSpecMap() {
      return this.defaultCastSpecMap;
   }

   public Map<String, Mapping> getSourceTypeToMappingMap() {
      return this.sourceTypeToMappingMap;
   }
}
