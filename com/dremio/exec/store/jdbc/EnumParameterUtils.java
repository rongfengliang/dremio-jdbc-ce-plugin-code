package com.dremio.exec.store.jdbc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

public final class EnumParameterUtils {
   public static final Map<String, TimeUnitRange> TIME_UNIT_MAPPING;

   private EnumParameterUtils() {
   }

   public static boolean hasTimeUnitAsFirstParam(List<RexNode> operands) {
      RexLiteral firstAsLiteral;
      if (!isFirstParamAFlag(operands)) {
         if (!operands.isEmpty() && operands.get(0) instanceof RexLiteral) {
            firstAsLiteral = (RexLiteral)operands.get(0);
            String unit = (String)firstAsLiteral.getValueAs(String.class);
            return unit == null ? false : TIME_UNIT_MAPPING.containsKey(unit.toLowerCase(Locale.ROOT));
         } else {
            return false;
         }
      } else {
         firstAsLiteral = (RexLiteral)operands.get(0);
         return firstAsLiteral.getValue() instanceof TimeUnit || firstAsLiteral.getValue() instanceof TimeUnitRange;
      }
   }

   public static TimeUnitRange getFirstParamAsTimeUnitRange(List<RexNode> operands) {
      Preconditions.checkArgument(hasTimeUnitAsFirstParam(operands));
      RexLiteral firstAsLiteral = (RexLiteral)operands.get(0);
      if (firstAsLiteral.getValue() instanceof TimeUnitRange) {
         return (TimeUnitRange)firstAsLiteral.getValue();
      } else if (firstAsLiteral.getValue() instanceof TimeUnit) {
         TimeUnitRange range = TimeUnitRange.of((TimeUnit)firstAsLiteral.getValueAs(TimeUnit.class), (TimeUnit)null);
         Preconditions.checkNotNull(range, "Time unit range must be constructed correctly.");
         return range;
      } else {
         String unit = (String)firstAsLiteral.getValueAs(String.class);
         Preconditions.checkNotNull(unit, "Time unit range must be constructed correctly.");
         return (TimeUnitRange)TIME_UNIT_MAPPING.get(unit.toLowerCase(Locale.ROOT));
      }
   }

   public static boolean isFirstParamAFlag(List<RexNode> operands) {
      if (operands.isEmpty()) {
         return false;
      } else {
         RexNode firstOperand = (RexNode)operands.get(0);
         if (firstOperand.getKind() != SqlKind.LITERAL) {
            return false;
         } else {
            RexLiteral firstAsLiteral = (RexLiteral)firstOperand;
            return firstAsLiteral.getTypeName().isSpecial();
         }
      }
   }

   static {
      Builder<String, TimeUnitRange> timeUnitBuilder = ImmutableMap.builder();
      TIME_UNIT_MAPPING = timeUnitBuilder.put("day", TimeUnitRange.DAY).put("hour", TimeUnitRange.HOUR).put("minute", TimeUnitRange.MINUTE).put("second", TimeUnitRange.SECOND).put("week", TimeUnitRange.WEEK).put("year", TimeUnitRange.YEAR).put("month", TimeUnitRange.MONTH).put("quarter", TimeUnitRange.QUARTER).put("decade", TimeUnitRange.DECADE).put("century", TimeUnitRange.CENTURY).put("millennium", TimeUnitRange.MILLENNIUM).build();
   }
}
