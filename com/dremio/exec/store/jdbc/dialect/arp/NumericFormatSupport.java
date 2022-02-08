package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList.Builder;
import java.util.List;

public class NumericFormatSupport {
   private final List<NumericFormatSupport.NumericFormatMapping> numericFormatMappings;
   private final String escapeQuote;

   @JsonCreator
   NumericFormatSupport(@JsonProperty("digit") NumericFormatDescriptor digit, @JsonProperty("zero_digit") NumericFormatDescriptor zero, @JsonProperty("decimal") NumericFormatDescriptor decimal, @JsonProperty("group_separator") NumericFormatDescriptor group_separator, @JsonProperty("exponent_separator") NumericFormatDescriptor exponent_separator, @JsonProperty("quote_character") NumericFormatDescriptor quote_character) {
      this.numericFormatMappings = (new Builder()).add(new NumericFormatSupport.NumericFormatMapping("#", digit)).add(new NumericFormatSupport.NumericFormatMapping("0", zero)).add(new NumericFormatSupport.NumericFormatMapping(".", decimal)).add(new NumericFormatSupport.NumericFormatMapping(",", group_separator)).add(new NumericFormatSupport.NumericFormatMapping("E", exponent_separator)).build();
      this.escapeQuote = quote_character.getFormat();
   }

   public List<NumericFormatSupport.NumericFormatMapping> getNumericFormatMappings() {
      return this.numericFormatMappings;
   }

   public String getEscapeQuote() {
      return this.escapeQuote;
   }

   static class NumericFormatMapping {
      private final String dremioNumericFormatString;
      private final NumericFormatDescriptor sourceNumericFormat;
      private final boolean areFormatsEqual;

      NumericFormatMapping(String format, NumericFormatDescriptor mapping) {
         this.dremioNumericFormatString = format;
         this.sourceNumericFormat = mapping;
         this.areFormatsEqual = mapping != null && format.equals(mapping.getFormat());
      }

      public String getDremioNumericFormatString() {
         return this.dremioNumericFormatString;
      }

      public NumericFormatDescriptor getSourceNumericFormat() {
         return this.sourceNumericFormat;
      }

      public boolean areNumericFormatsEqual() {
         return this.areFormatsEqual;
      }
   }
}
