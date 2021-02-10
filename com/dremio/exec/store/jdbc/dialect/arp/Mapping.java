package com.dremio.exec.store.jdbc.dialect.arp;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.map.CaseInsensitiveMap;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.Map;

public class Mapping {
   static final Map<String, CompleteType> ARP_TO_DREMIOTYPE_MAP;
   @JsonIgnore
   private final DataType source;
   @JsonIgnore
   private final CompleteType dremio;
   @JsonIgnore
   private final boolean defaultCastSpec;
   @JsonIgnore
   private final Mapping.RequiredCastArgs requiredCastArgs;

   @JsonCreator
   Mapping(@JsonProperty("source") DataType source, @JsonProperty("dremio") DataType dremio, @JsonProperty("default_cast_spec") boolean defaultCastSpec, @JsonProperty("required_cast_args") String requiredCastArgs) {
      this.source = source;
      this.defaultCastSpec = defaultCastSpec;
      this.dremio = convertDremioTypeStringToCompleteType(dremio.getName(), source.getMaxPrecision() == null ? 0 : source.getMaxPrecision(), source.getMaxScale() == null ? 0 : source.getMaxScale());
      if (!Strings.isNullOrEmpty(requiredCastArgs)) {
         this.requiredCastArgs = Mapping.RequiredCastArgs.valueOf(requiredCastArgs.toUpperCase(Locale.ROOT));
      } else {
         this.requiredCastArgs = Mapping.RequiredCastArgs.NONE;
      }

   }

   @VisibleForTesting
   public static CompleteType convertDremioTypeStringToCompleteType(String dremioTypename) {
      return convertDremioTypeStringToCompleteType(dremioTypename, 0, 0);
   }

   @VisibleForTesting
   public static CompleteType convertDremioTypeStringToCompleteType(String dremioTypename, int precision, int scale) {
      CompleteType dremioType = (CompleteType)ARP_TO_DREMIOTYPE_MAP.get(dremioTypename);
      if (dremioType == null) {
         throw new RuntimeException(String.format("Invalid Dremio typename specified in ARP file: '%s'.", dremioTypename));
      } else {
         return dremioType.isDecimal() ? CompleteType.fromDecimalPrecisionScale(precision, scale) : dremioType;
      }
   }

   public CompleteType getDremio() {
      return this.dremio;
   }

   public DataType getSource() {
      return this.source;
   }

   public boolean isDefaultCastSpec() {
      return this.defaultCastSpec;
   }

   public Mapping.RequiredCastArgs getRequiredCastArgs() {
      return this.requiredCastArgs;
   }

   static {
      Map<String, CompleteType> interimMap = ImmutableMap.builder().put("bigint", CompleteType.BIGINT).put("boolean", CompleteType.BIT).put("date", CompleteType.DATE).put("decimal", CompleteType.DECIMAL).put("double", CompleteType.DOUBLE).put("float", CompleteType.FLOAT).put("integer", CompleteType.INT).put("interval_day_second", CompleteType.INTERVAL_DAY_SECONDS).put("interval_year_month", CompleteType.INTERVAL_YEAR_MONTHS).put("time", CompleteType.TIME).put("timestamp", CompleteType.TIMESTAMP).put("varbinary", CompleteType.VARBINARY).put("varchar", CompleteType.VARCHAR).build();
      ARP_TO_DREMIOTYPE_MAP = CaseInsensitiveMap.newImmutableMap(interimMap);
   }

   static enum RequiredCastArgs {
      NONE {
         public String serializeArguments(String sourceTypeName, int precision, int scale) {
            return MessageFormat.format("{0}", sourceTypeName);
         }
      },
      PRECISION {
         public String serializeArguments(String sourceTypeName, int precision, int scale) {
            return MessageFormat.format("{0}({1})", sourceTypeName, String.valueOf(precision));
         }
      },
      SCALE {
         public String serializeArguments(String sourceTypeName, int precision, int scale) {
            return MessageFormat.format("{0}({1})", sourceTypeName, String.valueOf(scale));
         }
      },
      PRECISION_SCALE {
         public String serializeArguments(String sourceTypeName, int precision, int scale) {
            return MessageFormat.format("{0}({1}, {2})", sourceTypeName, String.valueOf(precision), String.valueOf(scale));
         }
      };

      private RequiredCastArgs() {
      }

      static Mapping.RequiredCastArgs getRequiredArgsBasedOnInputs(boolean hasPrecision, boolean hasScale, Mapping.RequiredCastArgs userSpecifiedArgs) {
         Mapping.RequiredCastArgs args;
         if (hasPrecision && hasScale) {
            args = PRECISION_SCALE;
         } else if (hasPrecision) {
            args = PRECISION;
         } else if (hasScale) {
            args = SCALE;
         } else {
            args = NONE;
         }

         switch(args) {
         case PRECISION_SCALE:
            return userSpecifiedArgs;
         case PRECISION:
            if (userSpecifiedArgs != PRECISION_SCALE && userSpecifiedArgs != SCALE) {
               return userSpecifiedArgs;
            }

            return PRECISION;
         case SCALE:
            if (userSpecifiedArgs != PRECISION_SCALE && userSpecifiedArgs != PRECISION) {
               return userSpecifiedArgs;
            }

            return SCALE;
         case NONE:
            return NONE;
         default:
            return NONE;
         }
      }

      public abstract String serializeArguments(String var1, int var2, int var3);

      // $FF: synthetic method
      RequiredCastArgs(Object x2) {
         this();
      }
   }
}
