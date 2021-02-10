package com.dremio.exec.store.jdbc.rel2sql;

import com.dremio.exec.store.jdbc.legacy.JdbcDremioSqlDialect;

public class JdbcDremioRelToSqlConverterBase extends JdbcDremioRelToSqlConverter {
   public JdbcDremioRelToSqlConverterBase(JdbcDremioSqlDialect dremioDialect) {
      super(dremioDialect);
   }

   protected JdbcDremioRelToSqlConverter getJdbcDremioRelToSqlConverter() {
      return this;
   }
}
