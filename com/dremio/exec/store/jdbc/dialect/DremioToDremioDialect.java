package com.dremio.exec.store.jdbc.dialect;

import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import com.dremio.exec.store.jdbc.rel2sql.DremioToDremioRelToSqlConverter;

public class DremioToDremioDialect extends ArpDialect {
   public DremioToDremioDialect(ArpYaml yaml) {
      super(yaml);
   }

   public DremioToDremioRelToSqlConverter getConverter() {
      return new DremioToDremioRelToSqlConverter(this);
   }
}
