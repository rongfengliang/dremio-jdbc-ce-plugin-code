package com.dremio.exec.store.jdbc.dialect;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpTypeMapper;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;
import com.dremio.exec.store.jdbc.rel2sql.RedshiftRelToSqlConverter;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.UnmodifiableIterator;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.core.Window.Group;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.RedshiftSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public final class RedshiftDialect extends ArpDialect {
   private final ArpTypeMapper typeMapper;

   public RedshiftDialect(ArpYaml yaml) {
      super(yaml);
      this.typeMapper = new RedshiftDialect.RedshiftTypeMapper(yaml);
   }

   public boolean supportsNestedAggregations() {
      return false;
   }

   public boolean supportsFunction(SqlOperator operator, RelDataType type, List<RelDataType> paramTypes) {
      return operator != SqlStdOperatorTable.DATETIME_MINUS && operator != SqlStdOperatorTable.DATETIME_PLUS ? super.supportsFunction(operator, type, paramTypes) : false;
   }

   public RedshiftRelToSqlConverter getConverter() {
      return new RedshiftRelToSqlConverter(this);
   }

   public ArpDialect.ArpSchemaFetcher newSchemaFetcher(JdbcPluginConfig config) {
      String query = String.format("SELECT * FROM (SELECT CURRENT_DATABASE() CAT, SCHEMANAME SCH, TABLENAME NME from pg_catalog.pg_tables UNION ALL SELECT CURRENT_DATABASE() CAT, SCHEMANAME SCH, VIEWNAME NME FROM pg_catalog.pg_views) t WHERE UPPER(SCH) NOT IN ('PG_CATALOG', '%s')", Joiner.on("','").join(config.getHiddenSchemas()));
      return new PostgreSQLDialect.PGSchemaFetcher(query, config);
   }

   protected boolean requiresAliasForFromItems() {
      return true;
   }

   public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
      if (call.getKind() == SqlKind.FLOOR && call.operandCount() == 2) {
         RedshiftSqlDialect.DEFAULT.unparseCall(writer, call, leftPrec, rightPrec);
      } else {
         super.unparseCall(writer, call, leftPrec, rightPrec);
      }

   }

   public SqlNode emulateNullDirection(SqlNode node, boolean nullsFirst, boolean desc) {
      return null;
   }

   public boolean supportsFetchOffsetInSetOperand() {
      return false;
   }

   public boolean useTimestampAddInsteadOfDatetimePlus() {
      return true;
   }

   public boolean removeDefaultWindowFrame(RexOver over) {
      return SqlKind.AGGREGATE.contains(over.getAggOperator().getKind());
   }

   public boolean supportsOver(RexOver over) {
      return over.getWindow() != null && over.getWindow().isRows();
   }

   public boolean supportsOver(Window window) {
      if (window.groups.isEmpty()) {
         return false;
      } else {
         UnmodifiableIterator var2 = window.groups.iterator();

         Group group;
         do {
            if (!var2.hasNext()) {
               return true;
            }

            group = (Group)var2.next();
         } while(group.isRows);

         return false;
      }
   }

   public boolean supportsLiteral(CompleteType type) {
      return !CompleteType.INTERVAL_DAY_SECONDS.equals(type) && !CompleteType.INTERVAL_YEAR_MONTHS.equals(type) ? super.supportsLiteral(type) : true;
   }

   public TypeMapper getDataTypeMapper() {
      return this.typeMapper;
   }

   private static class RedshiftTypeMapper extends ArpTypeMapper {
      public RedshiftTypeMapper(ArpYaml yaml) {
         super(yaml);
      }

      protected SourceTypeDescriptor createTypeDescriptor(TypeMapper.AddPropertyCallback addColumnPropertyCallback, TypeMapper.InvalidMetaDataCallback invalidMetaDataCallback, Connection connection, TypeMapper.TableIdentifier table, ResultSetMetaData metaData, String columnLabel, int colIndex) throws SQLException {
         Preconditions.checkNotNull(invalidMetaDataCallback);
         int colType = metaData.getColumnType(colIndex);
         if ((colType == 2 || colType == 3) && metaData.getPrecision(colIndex) <= 0) {
            String badPrecisionMessage = "Redshift server returned invalid metadata for a numeric or decimal operation.";
            invalidMetaDataCallback.throwOnInvalidMetaData("Redshift server returned invalid metadata for a numeric or decimal operation.");
            throw new IllegalArgumentException("invalidMetaDataCallbacks must throw an exception.");
         } else {
            return super.createTypeDescriptor(addColumnPropertyCallback, invalidMetaDataCallback, connection, table, metaData, columnLabel, colIndex);
         }
      }
   }
}
