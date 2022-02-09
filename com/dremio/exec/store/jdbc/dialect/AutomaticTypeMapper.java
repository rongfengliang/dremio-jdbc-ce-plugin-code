package com.dremio.exec.store.jdbc.dialect;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.store.jdbc.ColumnPropertiesProcessors;
import com.dremio.exec.store.jdbc.JdbcTypeConverter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import java.util.Iterator;
import java.util.List;
import org.apache.arrow.vector.types.Types.MinorType;

public final class AutomaticTypeMapper extends TypeMapper {
   public static final AutomaticTypeMapper INSTANCE = new AutomaticTypeMapper();

   protected List<JdbcToFieldMapping> mapSourceToArrowFields(TypeMapper.UnrecognizedTypeCallback unrecognizedTypeCallback, TypeMapper.AddPropertyCallback addColumnPropertyCallback, List<SourceTypeDescriptor> columnInfo, boolean mapSkippedColumnsAsNullType) {
      Builder<JdbcToFieldMapping> builder = ImmutableList.builder();
      int jdbcColumnIndex = 0;
      JdbcTypeConverter converter = this.useDecimalToDoubleMapping() ? new JdbcTypeConverter.DecimalToDoubleJdbcTypeConverter() : new JdbcTypeConverter();
      Iterator var8 = columnInfo.iterator();

      while(var8.hasNext()) {
         SourceTypeDescriptor column = (SourceTypeDescriptor)var8.next();
         ++jdbcColumnIndex;
         MinorType minorType = ((JdbcTypeConverter)converter).getMinorType(column.getReportedJdbcType());
         if (minorType == null) {
            unrecognizedTypeCallback.mark(column, true);
         } else {
            JdbcToFieldMapping newMapping;
            if (minorType == MinorType.DECIMAL) {
               newMapping = new JdbcToFieldMapping(CompleteType.fromDecimalPrecisionScale(column.getPrecision(), column.getScale()), jdbcColumnIndex, column);
            } else {
               newMapping = new JdbcToFieldMapping(CompleteType.fromMinorType(MajorTypeHelper.getMinorTypeFromArrowMinorType(minorType)), jdbcColumnIndex, column);
            }

            if (addColumnPropertyCallback != null) {
               addColumnPropertyCallback.addProperty(newMapping.getField().getName(), ColumnPropertiesProcessors.SourceTypeNameProcessor.of(newMapping.getSourceTypeDescriptor().getDataSourceTypeName()));
            }

            builder.add(newMapping);
         }
      }

      return builder.build();
   }

   private AutomaticTypeMapper() {
      super(true);
   }
}
