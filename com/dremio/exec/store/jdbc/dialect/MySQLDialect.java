package com.dremio.exec.store.jdbc.dialect;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpTypeMapper;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import com.dremio.exec.store.jdbc.rel2sql.MySQLRelToSqlConverter;
import com.google.common.base.Joiner;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlDialect.CalendarPolicy;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MySQLDialect extends ArpDialect {
   private final ArpTypeMapper typeMapper;

   public MySQLDialect(ArpYaml yaml) {
      super(yaml);
      this.typeMapper = new MySQLDialect.MySQLTypeMapper(yaml);
   }

   public SqlNode emulateNullDirection(SqlNode node, boolean nullsFirst, boolean desc) {
      return MysqlSqlDialect.DEFAULT.emulateNullDirection(node, nullsFirst, desc);
   }

   public CalendarPolicy getCalendarPolicy() {
      return MysqlSqlDialect.DEFAULT.getCalendarPolicy();
   }

   public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      if (call.getKind() == SqlKind.FLOOR && call.operandCount() == 2) {
         MysqlSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
      } else {
         super.unparseCall(writer, call, leftPrec, rightPrec);
      }

   }

   public boolean supportsLiteral(CompleteType type) {
      return type.isTemporal() ? true : super.supportsLiteral(type);
   }

   public SqlNode rewriteSingleValueExpr(SqlNode aggCall) {
      return MysqlSqlDialect.DEFAULT.rewriteSingleValueExpr(aggCall);
   }

   public boolean useTimestampAddInsteadOfDatetimePlus() {
      return true;
   }

   public MySQLRelToSqlConverter getConverter() {
      return new MySQLRelToSqlConverter(this);
   }

   public MySQLDialect.MySQLSchemaFetcher newSchemaFetcher(JdbcPluginConfig config) {
      String query = String.format("SELECT * FROM (SELECT TABLE_SCHEMA CAT, NULL SCH, TABLE_NAME NME from information_schema.tables WHERE TABLE_TYPE NOT IN ('%s')) t WHERE UPPER(CAT) NOT IN ('%s')", Joiner.on("','").join(config.getHiddenTableTypes()), Joiner.on("','").join(config.getHiddenSchemas()));
      return new MySQLDialect.MySQLSchemaFetcher(query, config);
   }

   public boolean supportsOver(RexOver over) {
      return false;
   }

   public boolean supportsOver(Window window) {
      return false;
   }

   public boolean supportsNestedAggregations() {
      return false;
   }

   public boolean coerceTimesToUTC() {
      return true;
   }

   public TypeMapper getDataTypeMapper() {
      return this.typeMapper;
   }

   private static class MySQLTypeMapper extends ArpTypeMapper {
      public MySQLTypeMapper(ArpYaml yaml) {
         super(yaml);
      }

      protected SourceTypeDescriptor createTypeDescriptor(TypeMapper.AddPropertyCallback addColumnPropertyCallback, TypeMapper.InvalidMetaDataCallback invalidMetaDataCallback, Connection connection, TypeMapper.TableIdentifier table, ResultSetMetaData metaData, String columnLabel, int colIndex) throws SQLException {
         int colType = metaData.getColumnType(colIndex);
         String colTypeName = -7 == colType ? "BIT" : metaData.getColumnTypeName(colIndex);
         int precision = metaData.getPrecision(colIndex);
         int scale = metaData.getScale(colIndex);
         if (colType == 3 || colType == 2) {
            precision = Math.max(precision, 38);
         }

         return new SourceTypeDescriptor(columnLabel, colType, colTypeName, colIndex, precision, scale);
      }
   }

   private static final class MySQLSchemaFetcher extends ArpDialect.ArpSchemaFetcher {
      private static final Logger logger = LoggerFactory.getLogger(MySQLDialect.MySQLSchemaFetcher.class);

      private MySQLSchemaFetcher(String query, JdbcPluginConfig config) {
         super(query, config);
      }

      protected long getRowCount(List<String> tablePath) {
         String sql = MessageFormat.format("SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = {0} AND TABLE_NAME = {1} AND ENGINE <> ''InnoDB''", this.config.getDialect().quoteStringLiteral((String)tablePath.get(0)), this.config.getDialect().quoteStringLiteral((String)tablePath.get(1)));
         Optional<Long> estimate = this.executeQueryAndGetFirstLong(sql);
         if (estimate.isPresent()) {
            return (Long)estimate.get();
         } else {
            logger.debug("Row count estimate {} detected on table {}. Retrying with count_big query.", 1000000000L, this.getQuotedPath(tablePath));
            return super.getRowCount(tablePath);
         }
      }

      // $FF: synthetic method
      MySQLSchemaFetcher(String x0, JdbcPluginConfig x1, Object x2) {
         this(x0, x1);
      }
   }
}
