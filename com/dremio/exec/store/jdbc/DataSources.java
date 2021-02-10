package com.dremio.exec.store.jdbc;

import com.google.common.base.Preconditions;
import java.sql.Driver;
import java.util.Properties;
import javax.sql.ConnectionPoolDataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.datasources.SharedPoolDataSource;

public final class DataSources {
   private DataSources() {
   }

   public static CloseableDataSource newGenericConnectionPoolDataSource(String driver, String url, String username, String password, Properties properties, DataSources.CommitMode commitMode) {
      Preconditions.checkNotNull(url);

      try {
         Class.forName((String)Preconditions.checkNotNull(driver)).asSubclass(Driver.class);
      } catch (ClassCastException | ClassNotFoundException var7) {
         throw new IllegalArgumentException(String.format("String '%s' does not denote a valid java.sql.Driver class name.", driver), var7);
      }

      BasicDataSource source = new BasicDataSource();
      source.setMaxTotal(Integer.MAX_VALUE);
      source.setTestOnBorrow(true);
      source.setValidationQueryTimeout(1);
      source.setDriverClassName(driver);
      source.setUrl(url);
      if (properties != null) {
         properties.forEach((name, value) -> {
            source.addConnectionProperty(name.toString(), value.toString());
         });
      }

      if (username != null) {
         source.setUsername(username);
      }

      if (password != null) {
         source.setPassword(password);
      }

      switch(commitMode) {
      case FORCE_AUTO_COMMIT_MODE:
         source.setDefaultAutoCommit(true);
         break;
      case FORCE_MANUAL_COMMIT_MODE:
         source.setDefaultAutoCommit(false);
      case DRIVER_SPECIFIED_COMMIT_MODE:
      }

      return CloseableDataSource.wrap(source);
   }

   public static CloseableDataSource newSharedDataSource(ConnectionPoolDataSource source) {
      SharedPoolDataSource ds = new SharedPoolDataSource();
      ds.setConnectionPoolDataSource(source);
      ds.setMaxTotal(Integer.MAX_VALUE);
      ds.setDefaultTestOnBorrow(true);
      ds.setValidationQueryTimeout(1);
      return CloseableDataSource.wrap(ds);
   }

   public static enum CommitMode {
      FORCE_AUTO_COMMIT_MODE,
      FORCE_MANUAL_COMMIT_MODE,
      DRIVER_SPECIFIED_COMMIT_MODE;
   }
}
