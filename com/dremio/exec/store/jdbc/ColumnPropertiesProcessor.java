package com.dremio.exec.store.jdbc;

import com.dremio.exec.store.jdbc.JdbcFetcherProto.ColumnProperties.Builder;

public interface ColumnPropertiesProcessor {
   void process(Builder var1);
}
