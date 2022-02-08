package com.dremio.exec.store.jdbc.dialect;

import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.JdbcSchemaFetcher;
import com.dremio.exec.store.jdbc.JdbcSchemaFetcherImpl;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import java.text.MessageFormat;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SynapseDialect extends MSSQLDialect {
   public SynapseDialect(ArpYaml yaml) {
      super(yaml);
   }

   public JdbcSchemaFetcher newSchemaFetcher(JdbcPluginConfig config) {
      return new SynapseDialect.SynapseSchemaFetcher(config);
   }

   private static final class SynapseSchemaFetcher extends JdbcSchemaFetcherImpl {
      private static final Logger logger = LoggerFactory.getLogger(SynapseDialect.SynapseSchemaFetcher.class);

      private SynapseSchemaFetcher(JdbcPluginConfig config) {
         super(config);
      }

      protected boolean usePrepareForColumnMetadata() {
         return true;
      }

      protected boolean usePrepareForGetTables() {
         return false;
      }

      public long getRowCount(List<String> tablePath) {
         String sql = MessageFormat.format("SELECT p.rows \nFROM {0}.sys.tables AS tbl\nINNER JOIN {0}.sys.indexes AS idx ON idx.object_id = tbl.object_id and idx.index_id < 2\nINNER JOIN {0}.sys.partitions AS p ON p.object_id=tbl.object_id\nAND p.index_id=idx.index_id\nWHERE ((tbl.name={2}\nAND SCHEMA_NAME(tbl.schema_id)={1}))", this.getConfig().getDialect().quoteIdentifier((String)tablePath.get(0)), this.getConfig().getDialect().quoteStringLiteral((String)tablePath.get(1)), this.getConfig().getDialect().quoteStringLiteral((String)tablePath.get(2)));
         String quotedPath = this.getQuotedPath(tablePath);
         Optional<Long> estimate = this.executeQueryAndGetFirstLong(sql);
         if (estimate.isPresent()) {
            return (Long)estimate.get();
         } else {
            logger.debug("Row count estimate not detected for table {}. Retrying with count_big query.", quotedPath);
            Optional<Long> fallbackEstimate = this.executeQueryAndGetFirstLong("SELECT COUNT_BIG(*) FROM " + quotedPath);
            return (Long)fallbackEstimate.orElse(1000000000L);
         }
      }

      // $FF: synthetic method
      SynapseSchemaFetcher(JdbcPluginConfig x0, Object x1) {
         this(x0);
      }
   }
}
