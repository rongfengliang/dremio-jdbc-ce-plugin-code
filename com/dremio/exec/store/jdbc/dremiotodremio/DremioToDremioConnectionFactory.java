package com.dremio.exec.store.jdbc.dremiotodremio;

import com.dremio.exec.store.jdbc.conf.DremioToDremioConf;
import com.dremio.jdbc.impl.DriverImpl;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class DremioToDremioConnectionFactory implements KeyedPooledObjectFactory<String, Connection> {
   private final DremioToDremioConf conf;
   private final Consumer<DremioToDremioPooledConnection> closeHandler;
   private final Driver driver;

   public DremioToDremioConnectionFactory(DremioToDremioConf conf, Consumer<DremioToDremioPooledConnection> closeHandler) {
      this.conf = conf;
      this.closeHandler = closeHandler;
      this.driver = new DriverImpl();
   }

   @VisibleForTesting
   DremioToDremioConnectionFactory(DremioToDremioConf conf, Consumer<DremioToDremioPooledConnection> closeHandler, Driver driver) {
      this.conf = conf;
      this.closeHandler = closeHandler;
      this.driver = driver;
   }

   public void activateObject(String key, PooledObject<Connection> p) throws Exception {
   }

   public void destroyObject(String key, PooledObject<Connection> p) throws Exception {
   }

   public PooledObject<Connection> makeObject(String queryUser) throws Exception {
      Properties properties = new Properties();
      properties.put("user", this.conf.username);
      properties.put("password", this.conf.password);
      if (this.conf.useSsl) {
         properties.put("ssl", "true");
      }

      if (!Strings.isNullOrEmpty(queryUser)) {
         properties.put("impersonation_target", queryUser);
      }

      return new DefaultPooledObject(new DremioToDremioPooledConnection(this.driver.connect(this.generateConnectionString(), properties), this.closeHandler, queryUser));
   }

   public void passivateObject(String key, PooledObject<Connection> p) throws Exception {
   }

   public boolean validateObject(String key, PooledObject<Connection> p) {
      try {
         return ((Connection)p.getObject()).isValid(2);
      } catch (SQLException var4) {
         return false;
      }
   }

   protected String generateConnectionString() {
      StringBuilder connectionString = new StringBuilder();
      connectionString.append("jdbc:dremio:");
      switch(this.conf.hostType) {
      case DIRECT:
         connectionString.append("direct");
         break;
      case ZOOKEEPER:
         connectionString.append("zk");
         break;
      default:
         throw new IllegalArgumentException("Unknown value for endpoint type.");
      }

      connectionString.append("=");
      connectionString.append(this.conf.hostname);
      connectionString.append(":");
      connectionString.append(this.conf.port);
      return connectionString.toString();
   }
}
