package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.jdbc.legacy.JdbcDremioSqlDialect;

public abstract class DialectConf<T extends DialectConf<T, P>, P extends StoragePlugin> extends ConnectionConf<T, P> {
   public abstract JdbcDremioSqlDialect getDialect();
}
