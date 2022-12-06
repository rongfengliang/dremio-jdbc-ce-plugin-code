package com.dremio.exec.store.jdbc.dremiotodremio;

import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.conf.DremioToDremioConf;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;

public class DremioToDremioPoolDataSource implements DataSource, DataSources.DataSourceWithImpersonation, Closeable, AutoCloseable {
   private final DremioToDremioConf conf;
   private final GenericKeyedObjectPool<String, Connection> pool;

   public DremioToDremioPoolDataSource(DremioToDremioConf conf) {
      this.conf = conf;
      GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig();
      poolConfig.setMaxIdlePerKey(50);
      poolConfig.setMaxTotal(50);
      poolConfig.setMaxTotalPerKey(50);
      poolConfig.setTestOnBorrow(true);
      this.pool = new GenericKeyedObjectPool(new DremioToDremioConnectionFactory(conf, (c) -> {
         this.returnConnection(c.getQueryUser(), c);
      }), poolConfig);
   }

   private void returnConnection(String queryUser, Connection connection) {
      this.pool.returnObject(queryUser, connection);
   }

   public void close() throws IOException {
      this.pool.close();
   }

   public Connection getConnection() throws SQLException {
      return this.getConnectionWithImpersonation((String)null);
   }

   public Connection getConnection(String username, String password) throws SQLException {
      throw new SQLException("Unsupported");
   }

   public <T> T unwrap(Class<T> iface) throws SQLException {
      try {
         return iface.cast(this);
      } catch (ClassCastException var3) {
         throw new SQLException("Unsupported");
      }
   }

   public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return iface.isInstance(this);
   }

   public PrintWriter getLogWriter() throws SQLException {
      throw new SQLException("Unsupported");
   }

   public void setLogWriter(PrintWriter out) throws SQLException {
      throw new SQLException("Unsupported");
   }

   public void setLoginTimeout(int seconds) throws SQLException {
      throw new SQLException("Unsupported");
   }

   public int getLoginTimeout() throws SQLException {
      throw new SQLException("Unsupported");
   }

   public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException("Unsupported");
   }

   public Connection getConnectionWithImpersonation(String queryUser) throws SQLException {
      try {
         return queryUser != null && this.conf.userImpersonation && !this.conf.username.equals(queryUser) ? (Connection)this.pool.borrowObject(queryUser) : (Connection)this.pool.borrowObject("");
      } catch (Exception var3) {
         throw new SQLException("Unable to retrieve connection from pool", var3);
      }
   }
}
