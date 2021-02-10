package com.dremio.exec.store.jdbc.legacy;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect.DatabaseProduct;

public abstract class LegacyDialect extends JdbcDremioSqlDialect {
   protected LegacyDialect(DatabaseProduct databaseProduct, String databaseProductName, String identifierQuoteString, NullCollation nullCollation) {
      super(databaseProductName, identifierQuoteString, nullCollation);
   }
}
