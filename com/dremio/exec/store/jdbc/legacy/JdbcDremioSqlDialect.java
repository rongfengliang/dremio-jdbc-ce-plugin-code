package com.dremio.exec.store.jdbc.legacy;

import com.dremio.exec.store.jdbc.AbstractDremioSqlDialect;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcSchemaFetcher;
import com.dremio.exec.store.jdbc.JdbcSchemaFetcherImpl;
import com.dremio.exec.store.jdbc.dialect.AutomaticTypeMapper;
import com.dremio.exec.store.jdbc.dialect.TypeMapper;
import com.dremio.exec.store.jdbc.rel2sql.JdbcDremioRelToSqlConverter;
import com.dremio.exec.store.jdbc.rel2sql.JdbcDremioRelToSqlConverterBase;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect.DatabaseProduct;

public class JdbcDremioSqlDialect extends AbstractDremioSqlDialect {
   public static final JdbcDremioSqlDialect DERBY;

   public JdbcDremioRelToSqlConverter getConverter() {
      return new JdbcDremioRelToSqlConverterBase(this);
   }

   protected JdbcDremioSqlDialect(String databaseProductName, String identifierQuoteString, NullCollation nullCollation) {
      super(databaseProductName, identifierQuoteString, nullCollation);
   }

   public JdbcSchemaFetcher newSchemaFetcher(JdbcPluginConfig config) {
      return new JdbcSchemaFetcherImpl(config);
   }

   public TypeMapper getDataTypeMapper() {
      return AutomaticTypeMapper.INSTANCE;
   }

   static {
      DERBY = new JdbcDremioSqlDialect(DatabaseProduct.DERBY.name(), "\"", NullCollation.HIGH);
   }
}
