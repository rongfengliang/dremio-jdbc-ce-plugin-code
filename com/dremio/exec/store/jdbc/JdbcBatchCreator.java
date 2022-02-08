package com.dremio.exec.store.jdbc;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.CoercionReader;
import com.dremio.exec.store.parquet.RecordReaderIterator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;
import com.dremio.sabot.op.spi.ProducerOperator.Creator;

public class JdbcBatchCreator implements Creator<JdbcSubScan> {
   public ProducerOperator create(FragmentExecutionContext fragmentExecContext, OperatorContext context, JdbcSubScan subScan) throws ExecutionSetupException {
      JdbcStoragePlugin plugin = (JdbcStoragePlugin)fragmentExecContext.getStoragePlugin(subScan.getPluginId());
      JdbcSchemaFetcherImpl schemaFetcher = (JdbcSchemaFetcherImpl)plugin.getFetcher();
      JdbcPluginConfig config = plugin.getConfig();
      JdbcRecordReader innerReader = new JdbcRecordReader(context, schemaFetcher.getSource(), subScan.getSql(), config, subScan.getColumns(), fragmentExecContext.cancelled(), subScan.getPluginId().getCapabilities(), plugin.getDialect().getDataTypeMapper(config), subScan.getReferencedTables(), subScan.getSkippedColumns());
      CoercionReader reader = new CoercionReader(context, subScan.getColumns(), innerReader, subScan.getFullSchema());
      return new ScanOperator(subScan, context, RecordReaderIterator.from(reader));
   }
}
