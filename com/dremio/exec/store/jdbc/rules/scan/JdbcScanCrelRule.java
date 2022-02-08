package com.dremio.exec.store.jdbc.rules.scan;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;
import com.dremio.exec.store.jdbc.rel.JdbcTableScan;

public class JdbcScanCrelRule extends SourceLogicalConverter {
   public JdbcScanCrelRule(SourceType type) {
      super(type);
   }

   public Rel convertScan(ScanCrel scan) {
      JdbcTableScan tableScan = new JdbcTableScan(scan.getCluster(), scan.getTraitSet().replace(Rel.LOGICAL), scan.getTable(), scan.getPluginId(), scan.getTableMetadata(), scan.getProjectedColumns(), scan.getObservedRowcountAdjustment(), scan.isDirectNamespaceDescendent());
      return new JdbcCrel(tableScan.getCluster(), scan.getTraitSet().replace(Rel.LOGICAL), tableScan, scan.getPluginId());
   }
}
