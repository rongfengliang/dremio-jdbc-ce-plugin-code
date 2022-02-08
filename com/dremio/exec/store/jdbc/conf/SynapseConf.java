package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.dialect.SynapseDialect;
import com.google.common.annotations.VisibleForTesting;

@SourceType(
   value = "SYNAPSE",
   label = "Microsoft Azure Synapse Analytics",
   uiConfig = "synapse-layout.json",
   externalQuerySupported = true
)
public class SynapseConf extends MSSQLConf {
   private static final String ARP_FILENAME = "arp/implementation/synapse-arp.yaml";
   private static final SynapseDialect SYNAPSE_ARP_DIALECT = (SynapseDialect)AbstractArpConf.loadArpFile("arp/implementation/synapse-arp.yaml", SynapseDialect::new);

   public SynapseDialect getDialect() {
      return SYNAPSE_ARP_DIALECT;
   }

   @VisibleForTesting
   public static SynapseDialect getDialectSingleton() {
      return SYNAPSE_ARP_DIALECT;
   }
}
