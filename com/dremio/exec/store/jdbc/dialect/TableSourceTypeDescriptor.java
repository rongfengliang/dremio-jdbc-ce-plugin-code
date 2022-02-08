package com.dremio.exec.store.jdbc.dialect;

public class TableSourceTypeDescriptor extends SourceTypeDescriptor {
   private final String catalog;
   private final String schema;
   private final String table;

   TableSourceTypeDescriptor(String fieldName, int reportedJdbcType, String dataSourceName, String catalog, String schema, String table, int colIndex, int precision, int scale) {
      super(fieldName, reportedJdbcType, dataSourceName, colIndex, precision, scale);
      this.catalog = catalog;
      this.schema = schema;
      this.table = table;
   }

   public String getCatalog() {
      return this.catalog;
   }

   public String getSchema() {
      return this.schema;
   }

   public String getTable() {
      return this.table;
   }

   public String toString() {
      return String.format("Table identifier: %s.%s.%s%n", this.catalog, this.schema, this.table) + super.toString();
   }
}
