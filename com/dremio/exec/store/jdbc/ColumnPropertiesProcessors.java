package com.dremio.exec.store.jdbc;

import com.dremio.exec.store.jdbc.JdbcFetcherProto.ColumnProperties;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.ColumnProperties.Builder;
import com.dremio.exec.store.jdbc.proto.JdbcReaderProto;
import com.google.protobuf.BoolValue;
import com.google.protobuf.StringValue;
import java.util.ArrayList;
import java.util.List;

public final class ColumnPropertiesProcessors {
   public static final String UNPUSHABLE_KEY = "unpushable";
   public static final String TYPENAME_KEY = "sourceTypeName";
   public static final String EXPLICIT_CAST_KEY = "explicitCast";
   public static final ColumnPropertiesProcessors.EnableExplicitCast ENABLE_EXPLICIT_CAST = new ColumnPropertiesProcessors.EnableExplicitCast();
   public static final ColumnPropertiesProcessors.UnpushableColumn UNPUSHABLE_COLUMN = new ColumnPropertiesProcessors.UnpushableColumn();
   private static final BoolValue TRUE = BoolValue.of(true);

   public static JdbcReaderProto.ColumnProperties convert(ColumnProperties properties) {
      List<JdbcReaderProto.ColumnProperty> convertedProperties = new ArrayList();
      if (properties.hasUnpushable()) {
         convertedProperties.add(JdbcReaderProto.ColumnProperty.newBuilder().setKey("unpushable").setValue(Boolean.toString(properties.getUnpushable().getValue())).build());
      }

      if (properties.hasExplicitCast()) {
         convertedProperties.add(JdbcReaderProto.ColumnProperty.newBuilder().setKey("explicitCast").setValue(Boolean.toString(properties.getExplicitCast().getValue())).build());
      }

      if (properties.hasSourceTypeName()) {
         convertedProperties.add(JdbcReaderProto.ColumnProperty.newBuilder().setKey("sourceTypeName").setValue(properties.getSourceTypeName().getValue()).build());
      }

      return JdbcReaderProto.ColumnProperties.newBuilder().setColumnName(properties.getColumnName()).addAllProperties(convertedProperties).build();
   }

   private ColumnPropertiesProcessors() {
   }

   public static final class SourceTypeNameProcessor implements ColumnPropertiesProcessor {
      private final String value;

      private SourceTypeNameProcessor(String value) {
         this.value = value;
      }

      public void process(Builder builder) {
         builder.setSourceTypeName(StringValue.of(this.value));
      }

      public static ColumnPropertiesProcessors.SourceTypeNameProcessor of(String value) {
         return new ColumnPropertiesProcessors.SourceTypeNameProcessor(value);
      }
   }

   public static final class UnpushableColumn implements ColumnPropertiesProcessor {
      private UnpushableColumn() {
      }

      public void process(Builder builder) {
         builder.setUnpushable(ColumnPropertiesProcessors.TRUE);
      }

      // $FF: synthetic method
      UnpushableColumn(Object x0) {
         this();
      }
   }

   private static final class EnableExplicitCast implements ColumnPropertiesProcessor {
      private EnableExplicitCast() {
      }

      public void process(Builder builder) {
         builder.setExplicitCast(ColumnPropertiesProcessors.TRUE);
      }

      // $FF: synthetic method
      EnableExplicitCast(Object x0) {
         this();
      }
   }
}
