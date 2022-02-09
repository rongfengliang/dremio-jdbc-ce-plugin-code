package com.dremio.exec.store.jdbc.dialect.arp;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.store.jdbc.ColumnPropertiesProcessors;
import com.dremio.exec.store.jdbc.dialect.JdbcToFieldMapping;
import com.dremio.exec.store.jdbc.dialect.SourceTypeDescriptor;
import com.dremio.exec.store.jdbc.dialect.TypeMapper;
import com.dremio.exec.work.foreman.UnsupportedDataTypeException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import java.util.Iterator;
import java.util.List;

public class ArpTypeMapper extends TypeMapper {
   private final ArpYaml yaml;

   protected ArpTypeMapper(ArpYaml yaml) {
      super(false);
      this.yaml = yaml;
   }

   private void mapSkippedColumnAsNullField(boolean mapSkippedColumnAsNullType, SourceTypeDescriptor column, Builder<JdbcToFieldMapping> builder) {
      if (mapSkippedColumnAsNullType) {
         builder.add(JdbcToFieldMapping.createSkippedField(column.getColumnIndex(), column));
      }

   }

   protected List<JdbcToFieldMapping> mapSourceToArrowFields(TypeMapper.UnrecognizedTypeCallback unrecognizedMappingCallback, TypeMapper.AddPropertyCallback addColumnPropertyCallback, List<SourceTypeDescriptor> columnInfo, boolean mapSkippedColumnsAsNullType) {
      Builder<JdbcToFieldMapping> builder = ImmutableList.builder();
      Iterator var6 = columnInfo.iterator();

      while(var6.hasNext()) {
         SourceTypeDescriptor column = (SourceTypeDescriptor)var6.next();

         try {
            if (this.shouldIgnore(column)) {
               unrecognizedMappingCallback.mark(column, true);
               this.mapSkippedColumnAsNullField(mapSkippedColumnsAsNullType, column, builder);
            } else {
               Mapping mapping = this.yaml.getMapping(column);
               CompleteType type;
               if (mapping == null) {
                  type = unrecognizedMappingCallback.mark(column, false);
               } else {
                  type = mapping.getDremio();
               }

               JdbcToFieldMapping newMapping = new JdbcToFieldMapping(type, column.getColumnIndex(), column);
               if (addColumnPropertyCallback != null) {
                  addColumnPropertyCallback.addProperty(newMapping.getField().getName(), ColumnPropertiesProcessors.SourceTypeNameProcessor.of(newMapping.getSourceTypeDescriptor().getDataSourceTypeName()));
               }

               builder.add(newMapping);
            }
         } catch (UnsupportedDataTypeException var11) {
            unrecognizedMappingCallback.mark(column, true);
            this.mapSkippedColumnAsNullField(mapSkippedColumnsAsNullType, column, builder);
         }
      }

      return builder.build();
   }

   protected boolean shouldIgnore(SourceTypeDescriptor column) {
      return false;
   }
}
