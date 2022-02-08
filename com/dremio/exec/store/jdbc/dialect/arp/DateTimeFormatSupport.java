package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList.Builder;
import java.util.List;

public class DateTimeFormatSupport {
   private final List<DateTimeFormatSupport.DateTimeFormatMapping> dateTimeFormatMappings;

   @JsonCreator
   DateTimeFormatSupport(@JsonProperty("era") DateTimeFormatDescriptor era, @JsonProperty("meridian") DateTimeFormatDescriptor meridian, @JsonProperty("century") DateTimeFormatDescriptor century, @JsonProperty("week_of_year") DateTimeFormatDescriptor week_of_year, @JsonProperty("day_of_week") DateTimeFormatDescriptor day_of_week, @JsonProperty("day_name_abbreviated") DateTimeFormatDescriptor day_name_abbreviated, @JsonProperty("day_name") DateTimeFormatDescriptor day_name, @JsonProperty("year_4") DateTimeFormatDescriptor year_4, @JsonProperty("year_2") DateTimeFormatDescriptor year_2, @JsonProperty("day_of_year") DateTimeFormatDescriptor day_of_year, @JsonProperty("month") DateTimeFormatDescriptor month, @JsonProperty("month_name_abbreviated") DateTimeFormatDescriptor month_name_abbreviated, @JsonProperty("month_name") DateTimeFormatDescriptor month_name, @JsonProperty("day_of_month") DateTimeFormatDescriptor day_of_month, @JsonProperty("hour_12") DateTimeFormatDescriptor hour_12, @JsonProperty("hour_24") DateTimeFormatDescriptor hour_24, @JsonProperty("minute") DateTimeFormatDescriptor minute, @JsonProperty("second") DateTimeFormatDescriptor second, @JsonProperty("millisecond") DateTimeFormatDescriptor millisecond, @JsonProperty("timezone_abbreviation") DateTimeFormatDescriptor timezone_abbreviation, @JsonProperty("timezone_offset") DateTimeFormatDescriptor timezone_offset) {
      this.dateTimeFormatMappings = (new Builder()).add(new DateTimeFormatSupport.DateTimeFormatMapping("DDD", day_of_year)).add(new DateTimeFormatSupport.DateTimeFormatMapping("DD", day_of_month)).add(new DateTimeFormatSupport.DateTimeFormatMapping("DAY", day_name)).add(new DateTimeFormatSupport.DateTimeFormatMapping("DY", day_name_abbreviated)).add(new DateTimeFormatSupport.DateTimeFormatMapping("YYYY", year_4)).add(new DateTimeFormatSupport.DateTimeFormatMapping("YY", year_2)).add(new DateTimeFormatSupport.DateTimeFormatMapping("AD", era)).add(new DateTimeFormatSupport.DateTimeFormatMapping("BC", era)).add(new DateTimeFormatSupport.DateTimeFormatMapping("CC", century)).add(new DateTimeFormatSupport.DateTimeFormatMapping("WW", week_of_year)).add(new DateTimeFormatSupport.DateTimeFormatMapping("MONTH", month_name)).add(new DateTimeFormatSupport.DateTimeFormatMapping("MON", month_name_abbreviated)).add(new DateTimeFormatSupport.DateTimeFormatMapping("MM", month)).add(new DateTimeFormatSupport.DateTimeFormatMapping("HH24", hour_24)).add(new DateTimeFormatSupport.DateTimeFormatMapping("HH12", hour_12)).add(new DateTimeFormatSupport.DateTimeFormatMapping("HH", hour_12)).add(new DateTimeFormatSupport.DateTimeFormatMapping("MI", minute)).add(new DateTimeFormatSupport.DateTimeFormatMapping("SS", second)).add(new DateTimeFormatSupport.DateTimeFormatMapping("AM", meridian)).add(new DateTimeFormatSupport.DateTimeFormatMapping("PM", meridian)).add(new DateTimeFormatSupport.DateTimeFormatMapping("FFF", millisecond)).add(new DateTimeFormatSupport.DateTimeFormatMapping("TZD", timezone_abbreviation)).add(new DateTimeFormatSupport.DateTimeFormatMapping("TZO", timezone_offset)).add(new DateTimeFormatSupport.DateTimeFormatMapping("D", day_of_week)).build();
   }

   public List<DateTimeFormatSupport.DateTimeFormatMapping> getDateTimeFormatMappings() {
      return this.dateTimeFormatMappings;
   }

   static class DateTimeFormatMapping {
      private final String dremioDateTimeFormatString;
      private final DateTimeFormatDescriptor sourceDateTimeFormat;
      private final boolean areFormatsEqual;

      DateTimeFormatMapping(String format, DateTimeFormatDescriptor mapping) {
         this.dremioDateTimeFormatString = format;
         this.sourceDateTimeFormat = mapping;
         this.areFormatsEqual = mapping != null && format.equals(mapping.getFormat());
      }

      public String getDremioDateTimeFormatString() {
         return this.dremioDateTimeFormatString;
      }

      public DateTimeFormatDescriptor getSourceDateTimeFormat() {
         return this.sourceDateTimeFormat;
      }

      public boolean areDateTimeFormatsEqual() {
         return this.areFormatsEqual;
      }
   }
}
