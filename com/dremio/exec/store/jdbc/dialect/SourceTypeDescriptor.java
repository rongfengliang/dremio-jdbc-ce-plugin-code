package com.dremio.exec.store.jdbc.dialect;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.sql.TypeInferenceUtils;
import org.apache.calcite.rel.type.RelDataType;

public class SourceTypeDescriptor {
   private final String fieldName;
   private final int reportedJdbcType;
   private final String dataSourceTypeName;
   private final int columnIndex;
   private final int precision;
   private final int scale;

   SourceTypeDescriptor(String fieldName, int reportedJdbcType, String dataSourceName, int colIndex, int precision, int scale) {
      this.fieldName = fieldName;
      this.reportedJdbcType = reportedJdbcType;
      this.dataSourceTypeName = dataSourceName;
      this.columnIndex = colIndex;
      this.precision = precision;
      this.scale = scale;
   }

   public String getFieldName() {
      return this.fieldName;
   }

   public int getReportedJdbcType() {
      return this.reportedJdbcType;
   }

   public String getDataSourceTypeName() {
      return this.dataSourceTypeName;
   }

   public int getColumnIndex() {
      return this.columnIndex;
   }

   public int getPrecision() {
      return this.precision;
   }

   public int getScale() {
      return this.scale;
   }

   public <T extends SourceTypeDescriptor> T unwrap(Class<T> iface) {
      return iface.isAssignableFrom(this.getClass()) ? this : null;
   }

   public String toString() {
      return String.format("Field Name: %s%n, JDBC type: %s%n, Data source type: %s", this.fieldName, TypeMapper.nameFromType(this.reportedJdbcType), this.dataSourceTypeName);
   }

   public static CompleteType getType(RelDataType relDataType) {
      return CalciteArrowHelper.fromRelAndMinorType(relDataType, TypeInferenceUtils.getMinorTypeFromCalciteType(relDataType));
   }
}
