package com.dremio.exec.store.jdbc.legacy;

import com.dremio.exec.store.jdbc.conf.AbstractArpConf;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public abstract class LegacyCapableJdbcConf<T extends AbstractArpConf<T>> extends AbstractArpConf<T> {
   public JdbcDremioSqlDialect getDialect() {
      ArpDialect arpDialect = this.getArpDialect();
      LegacyDialect legacyDialect = this.getLegacyDialect();
      Preconditions.checkArgument(arpDialect.getDatabaseProduct() == legacyDialect.getDatabaseProduct(), "Legacy dialect and ARP dialect should be for the same Database product.");
      return (JdbcDremioSqlDialect)(!this.getLegacyFlag() ? arpDialect : legacyDialect);
   }

   protected abstract ArpDialect getArpDialect();

   protected abstract LegacyDialect getLegacyDialect();

   protected abstract boolean getLegacyFlag();

   @VisibleForTesting
   public boolean supportsExternalQuery(boolean isExternalQueryEnabled) {
      return !this.getLegacyFlag() && isExternalQueryEnabled;
   }
}
