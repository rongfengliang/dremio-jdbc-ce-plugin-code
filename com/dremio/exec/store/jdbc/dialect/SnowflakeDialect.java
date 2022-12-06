package com.dremio.exec.store.jdbc.dialect;

import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcSchemaFetcherImpl;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlIntervalLiteral.IntervalValue;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SnowflakeDialect extends ArpDialect {
   public SnowflakeDialect(ArpYaml yaml) {
      super(yaml);
   }

   public JdbcSchemaFetcherImpl newSchemaFetcher(JdbcPluginConfig config) {
      return new SnowflakeDialect.SnowflakeSchemaFetcher(config);
   }

   public void unparseSqlIntervalLiteral(SqlWriter writer, SqlIntervalLiteral literal, int leftPrec, int rightPrec) {
      IntervalValue interval = (IntervalValue)literal.getValue();
      String intervalString = literal.getValue().toString();
      SqlIntervalQualifier qualifier;
      if (interval.getIntervalQualifier().isSingleDatetimeField()) {
         switch(interval.getIntervalQualifier().getStartUnit()) {
         case WEEK:
            qualifier = new SqlIntervalQualifier(TimeUnit.DAY, (TimeUnit)null, SqlParserPos.ZERO);
            break;
         case QUARTER:
            qualifier = new SqlIntervalQualifier(TimeUnit.MONTH, (TimeUnit)null, SqlParserPos.ZERO);
            break;
         default:
            qualifier = interval.getIntervalQualifier();
         }
      } else {
         qualifier = interval.getIntervalQualifier();
      }

      writer.keyword("INTERVAL");
      if (interval.getSign() == -1) {
         writer.print("-");
      }

      writer.literal("'" + intervalString);
      this.unparseSqlIntervalQualifier(writer, qualifier, RelDataTypeSystem.DEFAULT);
      writer.literal("'");
   }

   public boolean supportsNestedAggregations() {
      return false;
   }

   private static class SnowflakeSchemaFetcher extends JdbcSchemaFetcherImpl {
      public SnowflakeSchemaFetcher(JdbcPluginConfig config) {
         super(config);
      }

      protected boolean usePrepareForColumnMetadata() {
         return true;
      }
   }
}
