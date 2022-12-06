package com.dremio.exec.store.jdbc.dremiotodremio;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

public class DremioToDremioPooledConnection implements Connection {
   private final Connection delegate;
   private final Consumer<DremioToDremioPooledConnection> closeHandler;
   private final String queryUser;

   public DremioToDremioPooledConnection(Connection delegate, Consumer<DremioToDremioPooledConnection> closeHandler, String queryUser) {
      this.delegate = delegate;
      this.closeHandler = closeHandler;
      this.queryUser = queryUser;
   }

   public Statement createStatement() throws SQLException {
      return this.delegate.createStatement();
   }

   public PreparedStatement prepareStatement(String sql) throws SQLException {
      return this.delegate.prepareStatement(sql);
   }

   public CallableStatement prepareCall(String sql) throws SQLException {
      return this.delegate.prepareCall(sql);
   }

   public String nativeSQL(String sql) throws SQLException {
      return this.delegate.nativeSQL(sql);
   }

   public void setAutoCommit(boolean autoCommit) throws SQLException {
      this.delegate.setAutoCommit(autoCommit);
   }

   public boolean getAutoCommit() throws SQLException {
      return this.delegate.getAutoCommit();
   }

   public void commit() throws SQLException {
      this.delegate.commit();
   }

   public void rollback() throws SQLException {
      this.delegate.rollback();
   }

   public void close() throws SQLException {
      this.closeHandler.accept(this);
   }

   public boolean isClosed() throws SQLException {
      return this.delegate.isClosed();
   }

   public DatabaseMetaData getMetaData() throws SQLException {
      return this.delegate.getMetaData();
   }

   public void setReadOnly(boolean readOnly) throws SQLException {
      this.delegate.setReadOnly(readOnly);
   }

   public boolean isReadOnly() throws SQLException {
      return this.delegate.isReadOnly();
   }

   public void setCatalog(String catalog) throws SQLException {
      this.delegate.setCatalog(catalog);
   }

   public String getCatalog() throws SQLException {
      return this.delegate.getCatalog();
   }

   public void setTransactionIsolation(int level) throws SQLException {
      this.delegate.setTransactionIsolation(level);
   }

   public int getTransactionIsolation() throws SQLException {
      return this.delegate.getTransactionIsolation();
   }

   public SQLWarning getWarnings() throws SQLException {
      return this.delegate.getWarnings();
   }

   public void clearWarnings() throws SQLException {
      this.delegate.clearWarnings();
   }

   public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
      return this.delegate.createStatement(resultSetType, resultSetConcurrency);
   }

   public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
      return this.delegate.prepareStatement(sql, resultSetType, resultSetConcurrency);
   }

   public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
      return this.delegate.prepareCall(sql, resultSetType, resultSetConcurrency);
   }

   public Map<String, Class<?>> getTypeMap() throws SQLException {
      return this.delegate.getTypeMap();
   }

   public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
      this.delegate.setTypeMap(map);
   }

   public void setHoldability(int holdability) throws SQLException {
      this.delegate.setHoldability(holdability);
   }

   public int getHoldability() throws SQLException {
      return this.delegate.getHoldability();
   }

   public Savepoint setSavepoint() throws SQLException {
      return this.delegate.setSavepoint();
   }

   public Savepoint setSavepoint(String name) throws SQLException {
      return this.delegate.setSavepoint(name);
   }

   public void rollback(Savepoint savepoint) throws SQLException {
      this.delegate.rollback();
   }

   public void releaseSavepoint(Savepoint savepoint) throws SQLException {
      this.delegate.releaseSavepoint(savepoint);
   }

   public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      return this.delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
   }

   public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      return this.delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
   }

   public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
      return this.delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
   }

   public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
      return this.delegate.prepareStatement(sql, autoGeneratedKeys);
   }

   public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
      return this.delegate.prepareStatement(sql, columnIndexes);
   }

   public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
      return this.delegate.prepareStatement(sql, columnNames);
   }

   public Clob createClob() throws SQLException {
      return this.delegate.createClob();
   }

   public Blob createBlob() throws SQLException {
      return this.delegate.createBlob();
   }

   public NClob createNClob() throws SQLException {
      return this.delegate.createNClob();
   }

   public SQLXML createSQLXML() throws SQLException {
      return this.delegate.createSQLXML();
   }

   public boolean isValid(int timeout) throws SQLException {
      return this.delegate.isValid(timeout);
   }

   public void setClientInfo(String name, String value) throws SQLClientInfoException {
      this.delegate.setClientInfo(name, value);
   }

   public void setClientInfo(Properties properties) throws SQLClientInfoException {
      this.delegate.setClientInfo(properties);
   }

   public String getClientInfo(String name) throws SQLException {
      return this.delegate.getClientInfo(name);
   }

   public Properties getClientInfo() throws SQLException {
      return this.delegate.getClientInfo();
   }

   public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
      return this.delegate.createArrayOf(typeName, elements);
   }

   public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
      return this.delegate.createStruct(typeName, attributes);
   }

   public void setSchema(String schema) throws SQLException {
      this.delegate.setSchema(schema);
   }

   public String getSchema() throws SQLException {
      return this.delegate.getSchema();
   }

   public void abort(Executor executor) throws SQLException {
      this.delegate.abort(executor);
   }

   public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
      this.delegate.setNetworkTimeout(executor, milliseconds);
   }

   public int getNetworkTimeout() throws SQLException {
      return this.delegate.getNetworkTimeout();
   }

   public <T> T unwrap(Class<T> iface) throws SQLException {
      return this.delegate.unwrap(iface);
   }

   public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return this.delegate.isWrapperFor(iface);
   }

   String getQueryUser() {
      return this.queryUser;
   }
}
