package com.dremio.exec.store.jdbc;

import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types.MinorType;

public class JdbcTypeConverter {
   private static final ImmutableMap<Integer, JdbcTypeConverter.SupportedJdbcType> JDBC_TYPE_MAPPINGS;

   public MinorType getMinorType(int jdbcType) {
      JdbcTypeConverter.SupportedJdbcType type = (JdbcTypeConverter.SupportedJdbcType)JDBC_TYPE_MAPPINGS.get(jdbcType);
      return type == null ? MinorType.VARCHAR : type.minorType;
   }

   static {
      JDBC_TYPE_MAPPINGS = ImmutableMap.builder().put(8, new JdbcTypeConverter.SupportedJdbcType(8, MinorType.FLOAT8)).put(6, new JdbcTypeConverter.SupportedJdbcType(6, MinorType.FLOAT4)).put(-6, new JdbcTypeConverter.SupportedJdbcType(4, MinorType.INT)).put(5, new JdbcTypeConverter.SupportedJdbcType(4, MinorType.INT)).put(4, new JdbcTypeConverter.SupportedJdbcType(4, MinorType.INT)).put(-5, new JdbcTypeConverter.SupportedJdbcType(-5, MinorType.BIGINT)).put(1, new JdbcTypeConverter.SupportedJdbcType(12, MinorType.VARCHAR)).put(12, new JdbcTypeConverter.SupportedJdbcType(12, MinorType.VARCHAR)).put(-1, new JdbcTypeConverter.SupportedJdbcType(12, MinorType.VARCHAR)).put(2005, new JdbcTypeConverter.SupportedJdbcType(12, MinorType.VARCHAR)).put(-15, new JdbcTypeConverter.SupportedJdbcType(12, MinorType.VARCHAR)).put(-9, new JdbcTypeConverter.SupportedJdbcType(12, MinorType.VARCHAR)).put(-16, new JdbcTypeConverter.SupportedJdbcType(12, MinorType.VARCHAR)).put(-3, new JdbcTypeConverter.SupportedJdbcType(-3, MinorType.VARBINARY)).put(-2, new JdbcTypeConverter.SupportedJdbcType(-3, MinorType.VARBINARY)).put(-4, new JdbcTypeConverter.SupportedJdbcType(-3, MinorType.VARBINARY)).put(2004, new JdbcTypeConverter.SupportedJdbcType(-3, MinorType.VARBINARY)).put(2, new JdbcTypeConverter.SupportedJdbcType(3, MinorType.DECIMAL)).put(3, new JdbcTypeConverter.SupportedJdbcType(3, MinorType.DECIMAL)).put(7, new JdbcTypeConverter.SupportedJdbcType(8, MinorType.FLOAT8)).put(91, new JdbcTypeConverter.SupportedJdbcType(91, MinorType.DATEMILLI)).put(92, new JdbcTypeConverter.SupportedJdbcType(92, MinorType.TIMEMILLI)).put(93, new JdbcTypeConverter.SupportedJdbcType(93, MinorType.TIMESTAMPMILLI)).put(16, new JdbcTypeConverter.SupportedJdbcType(16, MinorType.BIT)).put(-7, new JdbcTypeConverter.SupportedJdbcType(16, MinorType.BIT)).put(2000, new JdbcTypeConverter.SupportedJdbcType(12, MinorType.VARCHAR)).build();
   }

   public static class DecimalToDoubleJdbcTypeConverter extends JdbcTypeConverter {
      public MinorType getMinorType(int jdbcType) {
         return 3 != jdbcType && 2 != jdbcType ? super.getMinorType(jdbcType) : super.getMinorType(8);
      }
   }

   private static class SupportedJdbcType {
      private final int jdbcType;
      private final MinorType minorType;

      public SupportedJdbcType(int jdbcType, MinorType minorType) {
         this.jdbcType = jdbcType;
         this.minorType = minorType;
      }
   }
}
