package com.dremio.exec.store.jdbc.conf;

import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;

@Options
public final class JdbcConstants {
   public static final String JDBC_ROW_COUNT_QUERY_TIMEOUT = "store.jdbc.row_count_query_timeout_seconds";
   public static final LongValidator JDBC_ROW_COUNT_QUERY_TIMEOUT_VALIDATOR = new PositiveLongValidator("store.jdbc.row_count_query_timeout_seconds", 2147483647L, 5L);

   private JdbcConstants() {
   }
}
