package com.dremio.exec.store.jdbc.dialect;

import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcSchemaFetcherImpl;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.sql.SqlAbstractDateTimeLiteral;
import org.apache.calcite.sql.SqlWriter;

public class ADXDialect extends MSSQLDialect {
   public ADXDialect(ArpYaml yaml) {
      super(yaml);
   }

   public JdbcSchemaFetcherImpl newSchemaFetcher(JdbcPluginConfig config) {
      return new JdbcSchemaFetcherImpl(config);
   }

   public boolean supportsOver(Window window) {
      return false;
   }

   public void unparseDateTimeLiteral(SqlWriter writer, SqlAbstractDateTimeLiteral literal, int leftPrec, int rightPrec) {
      switch(literal.getTypeName()) {
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
         writer.literal("{ts'" + literal.toFormattedString() + "'}");
         break;
      case DATE:
         writer.literal("{d'" + literal.toFormattedString() + "'}");
         break;
      case TIME:
      case TIME_WITH_LOCAL_TIME_ZONE:
         writer.literal("{t'" + literal.toFormattedString() + "'}");
         break;
      default:
         writer.literal("'" + literal.toFormattedString() + "'");
      }

   }
}
