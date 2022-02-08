package com.dremio.exec.store.jdbc.proto;

import com.google.protobuf.AbstractParser;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.LazyStringArrayList;
import com.google.protobuf.LazyStringList;
import com.google.protobuf.Message;
import com.google.protobuf.MessageLite;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Parser;
import com.google.protobuf.ProtocolStringList;
import com.google.protobuf.RepeatedFieldBuilderV3;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.GeneratedMessageV3.BuilderParent;
import com.google.protobuf.GeneratedMessageV3.FieldAccessorTable;
import com.google.protobuf.GeneratedMessageV3.UnusedPrivateParameter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class JdbcReaderProto {
   private static final Descriptor internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_descriptor;
   private static final FieldAccessorTable internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_fieldAccessorTable;
   private static final Descriptor internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_descriptor;
   private static final FieldAccessorTable internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_fieldAccessorTable;
   private static final Descriptor internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_descriptor;
   private static final FieldAccessorTable internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_fieldAccessorTable;
   private static FileDescriptor descriptor;

   private JdbcReaderProto() {
   }

   public static void registerAllExtensions(ExtensionRegistryLite registry) {
   }

   public static void registerAllExtensions(ExtensionRegistry registry) {
      registerAllExtensions((ExtensionRegistryLite)registry);
   }

   public static FileDescriptor getDescriptor() {
      return descriptor;
   }

   static {
      String[] descriptorData = new String[]{"\n\njdbc.proto\u0012 com.dremio.exec.store.jdbc.proto\",\n\u000eColumnProperty\u0012\u000b\n\u0003key\u0018\u0001 \u0001(\t\u0012\r\n\u0005value\u0018\u0002 \u0001(\t\"m\n\u0010ColumnProperties\u0012\u0013\n\u000bcolumn_name\u0018\u0001 \u0001(\t\u0012D\n\nproperties\u0018\u0002 \u0003(\u000b20.com.dremio.exec.store.jdbc.proto.ColumnProperty\"x\n\u000eJdbcTableXattr\u0012\u0017\n\u000fskipped_columns\u0018\u0001 \u0003(\t\u0012M\n\u0011column_properties\u0018\u0002 \u0003(\u000b22.com.dremio.exec.store.jdbc.proto.ColumnPropertiesB5\n com.dremio.exec.store.jdbc.protoB\u000fJdbcReaderProtoH\u0001"};
      descriptor = FileDescriptor.internalBuildGeneratedFileFrom(descriptorData, new FileDescriptor[0]);
      internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_descriptor = (Descriptor)getDescriptor().getMessageTypes().get(0);
      internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_fieldAccessorTable = new FieldAccessorTable(internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_descriptor, new String[]{"Key", "Value"});
      internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_descriptor = (Descriptor)getDescriptor().getMessageTypes().get(1);
      internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_fieldAccessorTable = new FieldAccessorTable(internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_descriptor, new String[]{"ColumnName", "Properties"});
      internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_descriptor = (Descriptor)getDescriptor().getMessageTypes().get(2);
      internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_fieldAccessorTable = new FieldAccessorTable(internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_descriptor, new String[]{"SkippedColumns", "ColumnProperties"});
   }

   public static final class JdbcTableXattr extends GeneratedMessageV3 implements JdbcReaderProto.JdbcTableXattrOrBuilder {
      private static final long serialVersionUID = 0L;
      public static final int SKIPPED_COLUMNS_FIELD_NUMBER = 1;
      private LazyStringList skippedColumns_;
      public static final int COLUMN_PROPERTIES_FIELD_NUMBER = 2;
      private List<JdbcReaderProto.ColumnProperties> columnProperties_;
      private byte memoizedIsInitialized;
      private static final JdbcReaderProto.JdbcTableXattr DEFAULT_INSTANCE = new JdbcReaderProto.JdbcTableXattr();
      /** @deprecated */
      @Deprecated
      public static final Parser<JdbcReaderProto.JdbcTableXattr> PARSER = new AbstractParser<JdbcReaderProto.JdbcTableXattr>() {
         public JdbcReaderProto.JdbcTableXattr parsePartialFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return new JdbcReaderProto.JdbcTableXattr(input, extensionRegistry);
         }
      };

      private JdbcTableXattr(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
         super(builder);
         this.memoizedIsInitialized = -1;
      }

      private JdbcTableXattr() {
         this.memoizedIsInitialized = -1;
         this.skippedColumns_ = LazyStringArrayList.EMPTY;
         this.columnProperties_ = Collections.emptyList();
      }

      protected Object newInstance(UnusedPrivateParameter unused) {
         return new JdbcReaderProto.JdbcTableXattr();
      }

      public final UnknownFieldSet getUnknownFields() {
         return this.unknownFields;
      }

      private JdbcTableXattr(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         this();
         if (extensionRegistry == null) {
            throw new NullPointerException();
         } else {
            int mutable_bitField0_ = 0;
            com.google.protobuf.UnknownFieldSet.Builder unknownFields = UnknownFieldSet.newBuilder();

            try {
               boolean done = false;

               while(!done) {
                  int tag = input.readTag();
                  switch(tag) {
                  case 0:
                     done = true;
                     break;
                  case 10:
                     ByteString bs = input.readBytes();
                     if ((mutable_bitField0_ & 1) == 0) {
                        this.skippedColumns_ = new LazyStringArrayList();
                        mutable_bitField0_ |= 1;
                     }

                     this.skippedColumns_.add(bs);
                     break;
                  case 18:
                     if ((mutable_bitField0_ & 2) == 0) {
                        this.columnProperties_ = new ArrayList();
                        mutable_bitField0_ |= 2;
                     }

                     this.columnProperties_.add((JdbcReaderProto.ColumnProperties)input.readMessage(JdbcReaderProto.ColumnProperties.PARSER, extensionRegistry));
                     break;
                  default:
                     if (!this.parseUnknownField(input, unknownFields, extensionRegistry, tag)) {
                        done = true;
                     }
                  }
               }
            } catch (InvalidProtocolBufferException var12) {
               throw var12.setUnfinishedMessage(this);
            } catch (IOException var13) {
               throw (new InvalidProtocolBufferException(var13)).setUnfinishedMessage(this);
            } finally {
               if ((mutable_bitField0_ & 1) != 0) {
                  this.skippedColumns_ = this.skippedColumns_.getUnmodifiableView();
               }

               if ((mutable_bitField0_ & 2) != 0) {
                  this.columnProperties_ = Collections.unmodifiableList(this.columnProperties_);
               }

               this.unknownFields = unknownFields.build();
               this.makeExtensionsImmutable();
            }

         }
      }

      public static final Descriptor getDescriptor() {
         return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_descriptor;
      }

      protected FieldAccessorTable internalGetFieldAccessorTable() {
         return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_fieldAccessorTable.ensureFieldAccessorsInitialized(JdbcReaderProto.JdbcTableXattr.class, JdbcReaderProto.JdbcTableXattr.Builder.class);
      }

      public ProtocolStringList getSkippedColumnsList() {
         return this.skippedColumns_;
      }

      public int getSkippedColumnsCount() {
         return this.skippedColumns_.size();
      }

      public String getSkippedColumns(int index) {
         return (String)this.skippedColumns_.get(index);
      }

      public ByteString getSkippedColumnsBytes(int index) {
         return this.skippedColumns_.getByteString(index);
      }

      public List<JdbcReaderProto.ColumnProperties> getColumnPropertiesList() {
         return this.columnProperties_;
      }

      public List<? extends JdbcReaderProto.ColumnPropertiesOrBuilder> getColumnPropertiesOrBuilderList() {
         return this.columnProperties_;
      }

      public int getColumnPropertiesCount() {
         return this.columnProperties_.size();
      }

      public JdbcReaderProto.ColumnProperties getColumnProperties(int index) {
         return (JdbcReaderProto.ColumnProperties)this.columnProperties_.get(index);
      }

      public JdbcReaderProto.ColumnPropertiesOrBuilder getColumnPropertiesOrBuilder(int index) {
         return (JdbcReaderProto.ColumnPropertiesOrBuilder)this.columnProperties_.get(index);
      }

      public final boolean isInitialized() {
         byte isInitialized = this.memoizedIsInitialized;
         if (isInitialized == 1) {
            return true;
         } else if (isInitialized == 0) {
            return false;
         } else {
            this.memoizedIsInitialized = 1;
            return true;
         }
      }

      public void writeTo(CodedOutputStream output) throws IOException {
         int i;
         for(i = 0; i < this.skippedColumns_.size(); ++i) {
            GeneratedMessageV3.writeString(output, 1, this.skippedColumns_.getRaw(i));
         }

         for(i = 0; i < this.columnProperties_.size(); ++i) {
            output.writeMessage(2, (MessageLite)this.columnProperties_.get(i));
         }

         this.unknownFields.writeTo(output);
      }

      public int getSerializedSize() {
         int size = this.memoizedSize;
         if (size != -1) {
            return size;
         } else {
            int size = 0;
            int i = 0;

            for(int i = 0; i < this.skippedColumns_.size(); ++i) {
               i += computeStringSizeNoTag(this.skippedColumns_.getRaw(i));
            }

            size = size + i;
            size += 1 * this.getSkippedColumnsList().size();

            for(i = 0; i < this.columnProperties_.size(); ++i) {
               size += CodedOutputStream.computeMessageSize(2, (MessageLite)this.columnProperties_.get(i));
            }

            size += this.unknownFields.getSerializedSize();
            this.memoizedSize = size;
            return size;
         }
      }

      public boolean equals(Object obj) {
         if (obj == this) {
            return true;
         } else if (!(obj instanceof JdbcReaderProto.JdbcTableXattr)) {
            return super.equals(obj);
         } else {
            JdbcReaderProto.JdbcTableXattr other = (JdbcReaderProto.JdbcTableXattr)obj;
            if (!this.getSkippedColumnsList().equals(other.getSkippedColumnsList())) {
               return false;
            } else if (!this.getColumnPropertiesList().equals(other.getColumnPropertiesList())) {
               return false;
            } else {
               return this.unknownFields.equals(other.unknownFields);
            }
         }
      }

      public int hashCode() {
         if (this.memoizedHashCode != 0) {
            return this.memoizedHashCode;
         } else {
            int hash = 41;
            int hash = 19 * hash + getDescriptor().hashCode();
            if (this.getSkippedColumnsCount() > 0) {
               hash = 37 * hash + 1;
               hash = 53 * hash + this.getSkippedColumnsList().hashCode();
            }

            if (this.getColumnPropertiesCount() > 0) {
               hash = 37 * hash + 2;
               hash = 53 * hash + this.getColumnPropertiesList().hashCode();
            }

            hash = 29 * hash + this.unknownFields.hashCode();
            this.memoizedHashCode = hash;
            return hash;
         }
      }

      public static JdbcReaderProto.JdbcTableXattr parseFrom(ByteBuffer data) throws InvalidProtocolBufferException {
         return (JdbcReaderProto.JdbcTableXattr)PARSER.parseFrom(data);
      }

      public static JdbcReaderProto.JdbcTableXattr parseFrom(ByteBuffer data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         return (JdbcReaderProto.JdbcTableXattr)PARSER.parseFrom(data, extensionRegistry);
      }

      public static JdbcReaderProto.JdbcTableXattr parseFrom(ByteString data) throws InvalidProtocolBufferException {
         return (JdbcReaderProto.JdbcTableXattr)PARSER.parseFrom(data);
      }

      public static JdbcReaderProto.JdbcTableXattr parseFrom(ByteString data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         return (JdbcReaderProto.JdbcTableXattr)PARSER.parseFrom(data, extensionRegistry);
      }

      public static JdbcReaderProto.JdbcTableXattr parseFrom(byte[] data) throws InvalidProtocolBufferException {
         return (JdbcReaderProto.JdbcTableXattr)PARSER.parseFrom(data);
      }

      public static JdbcReaderProto.JdbcTableXattr parseFrom(byte[] data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         return (JdbcReaderProto.JdbcTableXattr)PARSER.parseFrom(data, extensionRegistry);
      }

      public static JdbcReaderProto.JdbcTableXattr parseFrom(InputStream input) throws IOException {
         return (JdbcReaderProto.JdbcTableXattr)GeneratedMessageV3.parseWithIOException(PARSER, input);
      }

      public static JdbcReaderProto.JdbcTableXattr parseFrom(InputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
         return (JdbcReaderProto.JdbcTableXattr)GeneratedMessageV3.parseWithIOException(PARSER, input, extensionRegistry);
      }

      public static JdbcReaderProto.JdbcTableXattr parseDelimitedFrom(InputStream input) throws IOException {
         return (JdbcReaderProto.JdbcTableXattr)GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input);
      }

      public static JdbcReaderProto.JdbcTableXattr parseDelimitedFrom(InputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
         return (JdbcReaderProto.JdbcTableXattr)GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input, extensionRegistry);
      }

      public static JdbcReaderProto.JdbcTableXattr parseFrom(CodedInputStream input) throws IOException {
         return (JdbcReaderProto.JdbcTableXattr)GeneratedMessageV3.parseWithIOException(PARSER, input);
      }

      public static JdbcReaderProto.JdbcTableXattr parseFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
         return (JdbcReaderProto.JdbcTableXattr)GeneratedMessageV3.parseWithIOException(PARSER, input, extensionRegistry);
      }

      public JdbcReaderProto.JdbcTableXattr.Builder newBuilderForType() {
         return newBuilder();
      }

      public static JdbcReaderProto.JdbcTableXattr.Builder newBuilder() {
         return DEFAULT_INSTANCE.toBuilder();
      }

      public static JdbcReaderProto.JdbcTableXattr.Builder newBuilder(JdbcReaderProto.JdbcTableXattr prototype) {
         return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
      }

      public JdbcReaderProto.JdbcTableXattr.Builder toBuilder() {
         return this == DEFAULT_INSTANCE ? new JdbcReaderProto.JdbcTableXattr.Builder() : (new JdbcReaderProto.JdbcTableXattr.Builder()).mergeFrom(this);
      }

      protected JdbcReaderProto.JdbcTableXattr.Builder newBuilderForType(BuilderParent parent) {
         JdbcReaderProto.JdbcTableXattr.Builder builder = new JdbcReaderProto.JdbcTableXattr.Builder(parent);
         return builder;
      }

      public static JdbcReaderProto.JdbcTableXattr getDefaultInstance() {
         return DEFAULT_INSTANCE;
      }

      public static Parser<JdbcReaderProto.JdbcTableXattr> parser() {
         return PARSER;
      }

      public Parser<JdbcReaderProto.JdbcTableXattr> getParserForType() {
         return PARSER;
      }

      public JdbcReaderProto.JdbcTableXattr getDefaultInstanceForType() {
         return DEFAULT_INSTANCE;
      }

      // $FF: synthetic method
      JdbcTableXattr(com.google.protobuf.GeneratedMessageV3.Builder x0, Object x1) {
         this(x0);
      }

      // $FF: synthetic method
      JdbcTableXattr(CodedInputStream x0, ExtensionRegistryLite x1, Object x2) throws InvalidProtocolBufferException {
         this(x0, x1);
      }

      public static final class Builder extends com.google.protobuf.GeneratedMessageV3.Builder<JdbcReaderProto.JdbcTableXattr.Builder> implements JdbcReaderProto.JdbcTableXattrOrBuilder {
         private int bitField0_;
         private LazyStringList skippedColumns_;
         private List<JdbcReaderProto.ColumnProperties> columnProperties_;
         private RepeatedFieldBuilderV3<JdbcReaderProto.ColumnProperties, JdbcReaderProto.ColumnProperties.Builder, JdbcReaderProto.ColumnPropertiesOrBuilder> columnPropertiesBuilder_;

         public static final Descriptor getDescriptor() {
            return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_descriptor;
         }

         protected FieldAccessorTable internalGetFieldAccessorTable() {
            return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_fieldAccessorTable.ensureFieldAccessorsInitialized(JdbcReaderProto.JdbcTableXattr.class, JdbcReaderProto.JdbcTableXattr.Builder.class);
         }

         private Builder() {
            this.skippedColumns_ = LazyStringArrayList.EMPTY;
            this.columnProperties_ = Collections.emptyList();
            this.maybeForceBuilderInitialization();
         }

         private Builder(BuilderParent parent) {
            super(parent);
            this.skippedColumns_ = LazyStringArrayList.EMPTY;
            this.columnProperties_ = Collections.emptyList();
            this.maybeForceBuilderInitialization();
         }

         private void maybeForceBuilderInitialization() {
            if (JdbcReaderProto.JdbcTableXattr.alwaysUseFieldBuilders) {
               this.getColumnPropertiesFieldBuilder();
            }

         }

         public JdbcReaderProto.JdbcTableXattr.Builder clear() {
            super.clear();
            this.skippedColumns_ = LazyStringArrayList.EMPTY;
            this.bitField0_ &= -2;
            if (this.columnPropertiesBuilder_ == null) {
               this.columnProperties_ = Collections.emptyList();
               this.bitField0_ &= -3;
            } else {
               this.columnPropertiesBuilder_.clear();
            }

            return this;
         }

         public Descriptor getDescriptorForType() {
            return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_JdbcTableXattr_descriptor;
         }

         public JdbcReaderProto.JdbcTableXattr getDefaultInstanceForType() {
            return JdbcReaderProto.JdbcTableXattr.getDefaultInstance();
         }

         public JdbcReaderProto.JdbcTableXattr build() {
            JdbcReaderProto.JdbcTableXattr result = this.buildPartial();
            if (!result.isInitialized()) {
               throw newUninitializedMessageException(result);
            } else {
               return result;
            }
         }

         public JdbcReaderProto.JdbcTableXattr buildPartial() {
            JdbcReaderProto.JdbcTableXattr result = new JdbcReaderProto.JdbcTableXattr(this);
            int from_bitField0_ = this.bitField0_;
            if ((this.bitField0_ & 1) != 0) {
               this.skippedColumns_ = this.skippedColumns_.getUnmodifiableView();
               this.bitField0_ &= -2;
            }

            result.skippedColumns_ = this.skippedColumns_;
            if (this.columnPropertiesBuilder_ == null) {
               if ((this.bitField0_ & 2) != 0) {
                  this.columnProperties_ = Collections.unmodifiableList(this.columnProperties_);
                  this.bitField0_ &= -3;
               }

               result.columnProperties_ = this.columnProperties_;
            } else {
               result.columnProperties_ = this.columnPropertiesBuilder_.build();
            }

            this.onBuilt();
            return result;
         }

         public JdbcReaderProto.JdbcTableXattr.Builder clone() {
            return (JdbcReaderProto.JdbcTableXattr.Builder)super.clone();
         }

         public JdbcReaderProto.JdbcTableXattr.Builder setField(FieldDescriptor field, Object value) {
            return (JdbcReaderProto.JdbcTableXattr.Builder)super.setField(field, value);
         }

         public JdbcReaderProto.JdbcTableXattr.Builder clearField(FieldDescriptor field) {
            return (JdbcReaderProto.JdbcTableXattr.Builder)super.clearField(field);
         }

         public JdbcReaderProto.JdbcTableXattr.Builder clearOneof(OneofDescriptor oneof) {
            return (JdbcReaderProto.JdbcTableXattr.Builder)super.clearOneof(oneof);
         }

         public JdbcReaderProto.JdbcTableXattr.Builder setRepeatedField(FieldDescriptor field, int index, Object value) {
            return (JdbcReaderProto.JdbcTableXattr.Builder)super.setRepeatedField(field, index, value);
         }

         public JdbcReaderProto.JdbcTableXattr.Builder addRepeatedField(FieldDescriptor field, Object value) {
            return (JdbcReaderProto.JdbcTableXattr.Builder)super.addRepeatedField(field, value);
         }

         public JdbcReaderProto.JdbcTableXattr.Builder mergeFrom(Message other) {
            if (other instanceof JdbcReaderProto.JdbcTableXattr) {
               return this.mergeFrom((JdbcReaderProto.JdbcTableXattr)other);
            } else {
               super.mergeFrom(other);
               return this;
            }
         }

         public JdbcReaderProto.JdbcTableXattr.Builder mergeFrom(JdbcReaderProto.JdbcTableXattr other) {
            if (other == JdbcReaderProto.JdbcTableXattr.getDefaultInstance()) {
               return this;
            } else {
               if (!other.skippedColumns_.isEmpty()) {
                  if (this.skippedColumns_.isEmpty()) {
                     this.skippedColumns_ = other.skippedColumns_;
                     this.bitField0_ &= -2;
                  } else {
                     this.ensureSkippedColumnsIsMutable();
                     this.skippedColumns_.addAll(other.skippedColumns_);
                  }

                  this.onChanged();
               }

               if (this.columnPropertiesBuilder_ == null) {
                  if (!other.columnProperties_.isEmpty()) {
                     if (this.columnProperties_.isEmpty()) {
                        this.columnProperties_ = other.columnProperties_;
                        this.bitField0_ &= -3;
                     } else {
                        this.ensureColumnPropertiesIsMutable();
                        this.columnProperties_.addAll(other.columnProperties_);
                     }

                     this.onChanged();
                  }
               } else if (!other.columnProperties_.isEmpty()) {
                  if (this.columnPropertiesBuilder_.isEmpty()) {
                     this.columnPropertiesBuilder_.dispose();
                     this.columnPropertiesBuilder_ = null;
                     this.columnProperties_ = other.columnProperties_;
                     this.bitField0_ &= -3;
                     this.columnPropertiesBuilder_ = JdbcReaderProto.JdbcTableXattr.alwaysUseFieldBuilders ? this.getColumnPropertiesFieldBuilder() : null;
                  } else {
                     this.columnPropertiesBuilder_.addAllMessages(other.columnProperties_);
                  }
               }

               this.mergeUnknownFields(other.unknownFields);
               this.onChanged();
               return this;
            }
         }

         public final boolean isInitialized() {
            return true;
         }

         public JdbcReaderProto.JdbcTableXattr.Builder mergeFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
            JdbcReaderProto.JdbcTableXattr parsedMessage = null;

            try {
               parsedMessage = (JdbcReaderProto.JdbcTableXattr)JdbcReaderProto.JdbcTableXattr.PARSER.parsePartialFrom(input, extensionRegistry);
            } catch (InvalidProtocolBufferException var8) {
               parsedMessage = (JdbcReaderProto.JdbcTableXattr)var8.getUnfinishedMessage();
               throw var8.unwrapIOException();
            } finally {
               if (parsedMessage != null) {
                  this.mergeFrom(parsedMessage);
               }

            }

            return this;
         }

         private void ensureSkippedColumnsIsMutable() {
            if ((this.bitField0_ & 1) == 0) {
               this.skippedColumns_ = new LazyStringArrayList(this.skippedColumns_);
               this.bitField0_ |= 1;
            }

         }

         public ProtocolStringList getSkippedColumnsList() {
            return this.skippedColumns_.getUnmodifiableView();
         }

         public int getSkippedColumnsCount() {
            return this.skippedColumns_.size();
         }

         public String getSkippedColumns(int index) {
            return (String)this.skippedColumns_.get(index);
         }

         public ByteString getSkippedColumnsBytes(int index) {
            return this.skippedColumns_.getByteString(index);
         }

         public JdbcReaderProto.JdbcTableXattr.Builder setSkippedColumns(int index, String value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.ensureSkippedColumnsIsMutable();
               this.skippedColumns_.set(index, value);
               this.onChanged();
               return this;
            }
         }

         public JdbcReaderProto.JdbcTableXattr.Builder addSkippedColumns(String value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.ensureSkippedColumnsIsMutable();
               this.skippedColumns_.add(value);
               this.onChanged();
               return this;
            }
         }

         public JdbcReaderProto.JdbcTableXattr.Builder addAllSkippedColumns(Iterable<String> values) {
            this.ensureSkippedColumnsIsMutable();
            com.google.protobuf.AbstractMessageLite.Builder.addAll(values, this.skippedColumns_);
            this.onChanged();
            return this;
         }

         public JdbcReaderProto.JdbcTableXattr.Builder clearSkippedColumns() {
            this.skippedColumns_ = LazyStringArrayList.EMPTY;
            this.bitField0_ &= -2;
            this.onChanged();
            return this;
         }

         public JdbcReaderProto.JdbcTableXattr.Builder addSkippedColumnsBytes(ByteString value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.ensureSkippedColumnsIsMutable();
               this.skippedColumns_.add(value);
               this.onChanged();
               return this;
            }
         }

         private void ensureColumnPropertiesIsMutable() {
            if ((this.bitField0_ & 2) == 0) {
               this.columnProperties_ = new ArrayList(this.columnProperties_);
               this.bitField0_ |= 2;
            }

         }

         public List<JdbcReaderProto.ColumnProperties> getColumnPropertiesList() {
            return this.columnPropertiesBuilder_ == null ? Collections.unmodifiableList(this.columnProperties_) : this.columnPropertiesBuilder_.getMessageList();
         }

         public int getColumnPropertiesCount() {
            return this.columnPropertiesBuilder_ == null ? this.columnProperties_.size() : this.columnPropertiesBuilder_.getCount();
         }

         public JdbcReaderProto.ColumnProperties getColumnProperties(int index) {
            return this.columnPropertiesBuilder_ == null ? (JdbcReaderProto.ColumnProperties)this.columnProperties_.get(index) : (JdbcReaderProto.ColumnProperties)this.columnPropertiesBuilder_.getMessage(index);
         }

         public JdbcReaderProto.JdbcTableXattr.Builder setColumnProperties(int index, JdbcReaderProto.ColumnProperties value) {
            if (this.columnPropertiesBuilder_ == null) {
               if (value == null) {
                  throw new NullPointerException();
               }

               this.ensureColumnPropertiesIsMutable();
               this.columnProperties_.set(index, value);
               this.onChanged();
            } else {
               this.columnPropertiesBuilder_.setMessage(index, value);
            }

            return this;
         }

         public JdbcReaderProto.JdbcTableXattr.Builder setColumnProperties(int index, JdbcReaderProto.ColumnProperties.Builder builderForValue) {
            if (this.columnPropertiesBuilder_ == null) {
               this.ensureColumnPropertiesIsMutable();
               this.columnProperties_.set(index, builderForValue.build());
               this.onChanged();
            } else {
               this.columnPropertiesBuilder_.setMessage(index, builderForValue.build());
            }

            return this;
         }

         public JdbcReaderProto.JdbcTableXattr.Builder addColumnProperties(JdbcReaderProto.ColumnProperties value) {
            if (this.columnPropertiesBuilder_ == null) {
               if (value == null) {
                  throw new NullPointerException();
               }

               this.ensureColumnPropertiesIsMutable();
               this.columnProperties_.add(value);
               this.onChanged();
            } else {
               this.columnPropertiesBuilder_.addMessage(value);
            }

            return this;
         }

         public JdbcReaderProto.JdbcTableXattr.Builder addColumnProperties(int index, JdbcReaderProto.ColumnProperties value) {
            if (this.columnPropertiesBuilder_ == null) {
               if (value == null) {
                  throw new NullPointerException();
               }

               this.ensureColumnPropertiesIsMutable();
               this.columnProperties_.add(index, value);
               this.onChanged();
            } else {
               this.columnPropertiesBuilder_.addMessage(index, value);
            }

            return this;
         }

         public JdbcReaderProto.JdbcTableXattr.Builder addColumnProperties(JdbcReaderProto.ColumnProperties.Builder builderForValue) {
            if (this.columnPropertiesBuilder_ == null) {
               this.ensureColumnPropertiesIsMutable();
               this.columnProperties_.add(builderForValue.build());
               this.onChanged();
            } else {
               this.columnPropertiesBuilder_.addMessage(builderForValue.build());
            }

            return this;
         }

         public JdbcReaderProto.JdbcTableXattr.Builder addColumnProperties(int index, JdbcReaderProto.ColumnProperties.Builder builderForValue) {
            if (this.columnPropertiesBuilder_ == null) {
               this.ensureColumnPropertiesIsMutable();
               this.columnProperties_.add(index, builderForValue.build());
               this.onChanged();
            } else {
               this.columnPropertiesBuilder_.addMessage(index, builderForValue.build());
            }

            return this;
         }

         public JdbcReaderProto.JdbcTableXattr.Builder addAllColumnProperties(Iterable<? extends JdbcReaderProto.ColumnProperties> values) {
            if (this.columnPropertiesBuilder_ == null) {
               this.ensureColumnPropertiesIsMutable();
               com.google.protobuf.AbstractMessageLite.Builder.addAll(values, this.columnProperties_);
               this.onChanged();
            } else {
               this.columnPropertiesBuilder_.addAllMessages(values);
            }

            return this;
         }

         public JdbcReaderProto.JdbcTableXattr.Builder clearColumnProperties() {
            if (this.columnPropertiesBuilder_ == null) {
               this.columnProperties_ = Collections.emptyList();
               this.bitField0_ &= -3;
               this.onChanged();
            } else {
               this.columnPropertiesBuilder_.clear();
            }

            return this;
         }

         public JdbcReaderProto.JdbcTableXattr.Builder removeColumnProperties(int index) {
            if (this.columnPropertiesBuilder_ == null) {
               this.ensureColumnPropertiesIsMutable();
               this.columnProperties_.remove(index);
               this.onChanged();
            } else {
               this.columnPropertiesBuilder_.remove(index);
            }

            return this;
         }

         public JdbcReaderProto.ColumnProperties.Builder getColumnPropertiesBuilder(int index) {
            return (JdbcReaderProto.ColumnProperties.Builder)this.getColumnPropertiesFieldBuilder().getBuilder(index);
         }

         public JdbcReaderProto.ColumnPropertiesOrBuilder getColumnPropertiesOrBuilder(int index) {
            return this.columnPropertiesBuilder_ == null ? (JdbcReaderProto.ColumnPropertiesOrBuilder)this.columnProperties_.get(index) : (JdbcReaderProto.ColumnPropertiesOrBuilder)this.columnPropertiesBuilder_.getMessageOrBuilder(index);
         }

         public List<? extends JdbcReaderProto.ColumnPropertiesOrBuilder> getColumnPropertiesOrBuilderList() {
            return this.columnPropertiesBuilder_ != null ? this.columnPropertiesBuilder_.getMessageOrBuilderList() : Collections.unmodifiableList(this.columnProperties_);
         }

         public JdbcReaderProto.ColumnProperties.Builder addColumnPropertiesBuilder() {
            return (JdbcReaderProto.ColumnProperties.Builder)this.getColumnPropertiesFieldBuilder().addBuilder(JdbcReaderProto.ColumnProperties.getDefaultInstance());
         }

         public JdbcReaderProto.ColumnProperties.Builder addColumnPropertiesBuilder(int index) {
            return (JdbcReaderProto.ColumnProperties.Builder)this.getColumnPropertiesFieldBuilder().addBuilder(index, JdbcReaderProto.ColumnProperties.getDefaultInstance());
         }

         public List<JdbcReaderProto.ColumnProperties.Builder> getColumnPropertiesBuilderList() {
            return this.getColumnPropertiesFieldBuilder().getBuilderList();
         }

         private RepeatedFieldBuilderV3<JdbcReaderProto.ColumnProperties, JdbcReaderProto.ColumnProperties.Builder, JdbcReaderProto.ColumnPropertiesOrBuilder> getColumnPropertiesFieldBuilder() {
            if (this.columnPropertiesBuilder_ == null) {
               this.columnPropertiesBuilder_ = new RepeatedFieldBuilderV3(this.columnProperties_, (this.bitField0_ & 2) != 0, this.getParentForChildren(), this.isClean());
               this.columnProperties_ = null;
            }

            return this.columnPropertiesBuilder_;
         }

         public final JdbcReaderProto.JdbcTableXattr.Builder setUnknownFields(UnknownFieldSet unknownFields) {
            return (JdbcReaderProto.JdbcTableXattr.Builder)super.setUnknownFields(unknownFields);
         }

         public final JdbcReaderProto.JdbcTableXattr.Builder mergeUnknownFields(UnknownFieldSet unknownFields) {
            return (JdbcReaderProto.JdbcTableXattr.Builder)super.mergeUnknownFields(unknownFields);
         }

         // $FF: synthetic method
         Builder(Object x0) {
            this();
         }

         // $FF: synthetic method
         Builder(BuilderParent x0, Object x1) {
            this(x0);
         }
      }
   }

   public interface JdbcTableXattrOrBuilder extends MessageOrBuilder {
      List<String> getSkippedColumnsList();

      int getSkippedColumnsCount();

      String getSkippedColumns(int var1);

      ByteString getSkippedColumnsBytes(int var1);

      List<JdbcReaderProto.ColumnProperties> getColumnPropertiesList();

      JdbcReaderProto.ColumnProperties getColumnProperties(int var1);

      int getColumnPropertiesCount();

      List<? extends JdbcReaderProto.ColumnPropertiesOrBuilder> getColumnPropertiesOrBuilderList();

      JdbcReaderProto.ColumnPropertiesOrBuilder getColumnPropertiesOrBuilder(int var1);
   }

   public static final class ColumnProperties extends GeneratedMessageV3 implements JdbcReaderProto.ColumnPropertiesOrBuilder {
      private static final long serialVersionUID = 0L;
      private int bitField0_;
      public static final int COLUMN_NAME_FIELD_NUMBER = 1;
      private volatile Object columnName_;
      public static final int PROPERTIES_FIELD_NUMBER = 2;
      private List<JdbcReaderProto.ColumnProperty> properties_;
      private byte memoizedIsInitialized;
      private static final JdbcReaderProto.ColumnProperties DEFAULT_INSTANCE = new JdbcReaderProto.ColumnProperties();
      /** @deprecated */
      @Deprecated
      public static final Parser<JdbcReaderProto.ColumnProperties> PARSER = new AbstractParser<JdbcReaderProto.ColumnProperties>() {
         public JdbcReaderProto.ColumnProperties parsePartialFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return new JdbcReaderProto.ColumnProperties(input, extensionRegistry);
         }
      };

      private ColumnProperties(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
         super(builder);
         this.memoizedIsInitialized = -1;
      }

      private ColumnProperties() {
         this.memoizedIsInitialized = -1;
         this.columnName_ = "";
         this.properties_ = Collections.emptyList();
      }

      protected Object newInstance(UnusedPrivateParameter unused) {
         return new JdbcReaderProto.ColumnProperties();
      }

      public final UnknownFieldSet getUnknownFields() {
         return this.unknownFields;
      }

      private ColumnProperties(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         this();
         if (extensionRegistry == null) {
            throw new NullPointerException();
         } else {
            int mutable_bitField0_ = 0;
            com.google.protobuf.UnknownFieldSet.Builder unknownFields = UnknownFieldSet.newBuilder();

            try {
               boolean done = false;

               while(!done) {
                  int tag = input.readTag();
                  switch(tag) {
                  case 0:
                     done = true;
                     break;
                  case 10:
                     ByteString bs = input.readBytes();
                     this.bitField0_ |= 1;
                     this.columnName_ = bs;
                     break;
                  case 18:
                     if ((mutable_bitField0_ & 2) == 0) {
                        this.properties_ = new ArrayList();
                        mutable_bitField0_ |= 2;
                     }

                     this.properties_.add((JdbcReaderProto.ColumnProperty)input.readMessage(JdbcReaderProto.ColumnProperty.PARSER, extensionRegistry));
                     break;
                  default:
                     if (!this.parseUnknownField(input, unknownFields, extensionRegistry, tag)) {
                        done = true;
                     }
                  }
               }
            } catch (InvalidProtocolBufferException var12) {
               throw var12.setUnfinishedMessage(this);
            } catch (IOException var13) {
               throw (new InvalidProtocolBufferException(var13)).setUnfinishedMessage(this);
            } finally {
               if ((mutable_bitField0_ & 2) != 0) {
                  this.properties_ = Collections.unmodifiableList(this.properties_);
               }

               this.unknownFields = unknownFields.build();
               this.makeExtensionsImmutable();
            }

         }
      }

      public static final Descriptor getDescriptor() {
         return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_descriptor;
      }

      protected FieldAccessorTable internalGetFieldAccessorTable() {
         return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_fieldAccessorTable.ensureFieldAccessorsInitialized(JdbcReaderProto.ColumnProperties.class, JdbcReaderProto.ColumnProperties.Builder.class);
      }

      public boolean hasColumnName() {
         return (this.bitField0_ & 1) != 0;
      }

      public String getColumnName() {
         Object ref = this.columnName_;
         if (ref instanceof String) {
            return (String)ref;
         } else {
            ByteString bs = (ByteString)ref;
            String s = bs.toStringUtf8();
            if (bs.isValidUtf8()) {
               this.columnName_ = s;
            }

            return s;
         }
      }

      public ByteString getColumnNameBytes() {
         Object ref = this.columnName_;
         if (ref instanceof String) {
            ByteString b = ByteString.copyFromUtf8((String)ref);
            this.columnName_ = b;
            return b;
         } else {
            return (ByteString)ref;
         }
      }

      public List<JdbcReaderProto.ColumnProperty> getPropertiesList() {
         return this.properties_;
      }

      public List<? extends JdbcReaderProto.ColumnPropertyOrBuilder> getPropertiesOrBuilderList() {
         return this.properties_;
      }

      public int getPropertiesCount() {
         return this.properties_.size();
      }

      public JdbcReaderProto.ColumnProperty getProperties(int index) {
         return (JdbcReaderProto.ColumnProperty)this.properties_.get(index);
      }

      public JdbcReaderProto.ColumnPropertyOrBuilder getPropertiesOrBuilder(int index) {
         return (JdbcReaderProto.ColumnPropertyOrBuilder)this.properties_.get(index);
      }

      public final boolean isInitialized() {
         byte isInitialized = this.memoizedIsInitialized;
         if (isInitialized == 1) {
            return true;
         } else if (isInitialized == 0) {
            return false;
         } else {
            this.memoizedIsInitialized = 1;
            return true;
         }
      }

      public void writeTo(CodedOutputStream output) throws IOException {
         if ((this.bitField0_ & 1) != 0) {
            GeneratedMessageV3.writeString(output, 1, this.columnName_);
         }

         for(int i = 0; i < this.properties_.size(); ++i) {
            output.writeMessage(2, (MessageLite)this.properties_.get(i));
         }

         this.unknownFields.writeTo(output);
      }

      public int getSerializedSize() {
         int size = this.memoizedSize;
         if (size != -1) {
            return size;
         } else {
            size = 0;
            if ((this.bitField0_ & 1) != 0) {
               size += GeneratedMessageV3.computeStringSize(1, this.columnName_);
            }

            for(int i = 0; i < this.properties_.size(); ++i) {
               size += CodedOutputStream.computeMessageSize(2, (MessageLite)this.properties_.get(i));
            }

            size += this.unknownFields.getSerializedSize();
            this.memoizedSize = size;
            return size;
         }
      }

      public boolean equals(Object obj) {
         if (obj == this) {
            return true;
         } else if (!(obj instanceof JdbcReaderProto.ColumnProperties)) {
            return super.equals(obj);
         } else {
            JdbcReaderProto.ColumnProperties other = (JdbcReaderProto.ColumnProperties)obj;
            if (this.hasColumnName() != other.hasColumnName()) {
               return false;
            } else if (this.hasColumnName() && !this.getColumnName().equals(other.getColumnName())) {
               return false;
            } else if (!this.getPropertiesList().equals(other.getPropertiesList())) {
               return false;
            } else {
               return this.unknownFields.equals(other.unknownFields);
            }
         }
      }

      public int hashCode() {
         if (this.memoizedHashCode != 0) {
            return this.memoizedHashCode;
         } else {
            int hash = 41;
            int hash = 19 * hash + getDescriptor().hashCode();
            if (this.hasColumnName()) {
               hash = 37 * hash + 1;
               hash = 53 * hash + this.getColumnName().hashCode();
            }

            if (this.getPropertiesCount() > 0) {
               hash = 37 * hash + 2;
               hash = 53 * hash + this.getPropertiesList().hashCode();
            }

            hash = 29 * hash + this.unknownFields.hashCode();
            this.memoizedHashCode = hash;
            return hash;
         }
      }

      public static JdbcReaderProto.ColumnProperties parseFrom(ByteBuffer data) throws InvalidProtocolBufferException {
         return (JdbcReaderProto.ColumnProperties)PARSER.parseFrom(data);
      }

      public static JdbcReaderProto.ColumnProperties parseFrom(ByteBuffer data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         return (JdbcReaderProto.ColumnProperties)PARSER.parseFrom(data, extensionRegistry);
      }

      public static JdbcReaderProto.ColumnProperties parseFrom(ByteString data) throws InvalidProtocolBufferException {
         return (JdbcReaderProto.ColumnProperties)PARSER.parseFrom(data);
      }

      public static JdbcReaderProto.ColumnProperties parseFrom(ByteString data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         return (JdbcReaderProto.ColumnProperties)PARSER.parseFrom(data, extensionRegistry);
      }

      public static JdbcReaderProto.ColumnProperties parseFrom(byte[] data) throws InvalidProtocolBufferException {
         return (JdbcReaderProto.ColumnProperties)PARSER.parseFrom(data);
      }

      public static JdbcReaderProto.ColumnProperties parseFrom(byte[] data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         return (JdbcReaderProto.ColumnProperties)PARSER.parseFrom(data, extensionRegistry);
      }

      public static JdbcReaderProto.ColumnProperties parseFrom(InputStream input) throws IOException {
         return (JdbcReaderProto.ColumnProperties)GeneratedMessageV3.parseWithIOException(PARSER, input);
      }

      public static JdbcReaderProto.ColumnProperties parseFrom(InputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
         return (JdbcReaderProto.ColumnProperties)GeneratedMessageV3.parseWithIOException(PARSER, input, extensionRegistry);
      }

      public static JdbcReaderProto.ColumnProperties parseDelimitedFrom(InputStream input) throws IOException {
         return (JdbcReaderProto.ColumnProperties)GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input);
      }

      public static JdbcReaderProto.ColumnProperties parseDelimitedFrom(InputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
         return (JdbcReaderProto.ColumnProperties)GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input, extensionRegistry);
      }

      public static JdbcReaderProto.ColumnProperties parseFrom(CodedInputStream input) throws IOException {
         return (JdbcReaderProto.ColumnProperties)GeneratedMessageV3.parseWithIOException(PARSER, input);
      }

      public static JdbcReaderProto.ColumnProperties parseFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
         return (JdbcReaderProto.ColumnProperties)GeneratedMessageV3.parseWithIOException(PARSER, input, extensionRegistry);
      }

      public JdbcReaderProto.ColumnProperties.Builder newBuilderForType() {
         return newBuilder();
      }

      public static JdbcReaderProto.ColumnProperties.Builder newBuilder() {
         return DEFAULT_INSTANCE.toBuilder();
      }

      public static JdbcReaderProto.ColumnProperties.Builder newBuilder(JdbcReaderProto.ColumnProperties prototype) {
         return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
      }

      public JdbcReaderProto.ColumnProperties.Builder toBuilder() {
         return this == DEFAULT_INSTANCE ? new JdbcReaderProto.ColumnProperties.Builder() : (new JdbcReaderProto.ColumnProperties.Builder()).mergeFrom(this);
      }

      protected JdbcReaderProto.ColumnProperties.Builder newBuilderForType(BuilderParent parent) {
         JdbcReaderProto.ColumnProperties.Builder builder = new JdbcReaderProto.ColumnProperties.Builder(parent);
         return builder;
      }

      public static JdbcReaderProto.ColumnProperties getDefaultInstance() {
         return DEFAULT_INSTANCE;
      }

      public static Parser<JdbcReaderProto.ColumnProperties> parser() {
         return PARSER;
      }

      public Parser<JdbcReaderProto.ColumnProperties> getParserForType() {
         return PARSER;
      }

      public JdbcReaderProto.ColumnProperties getDefaultInstanceForType() {
         return DEFAULT_INSTANCE;
      }

      // $FF: synthetic method
      ColumnProperties(com.google.protobuf.GeneratedMessageV3.Builder x0, Object x1) {
         this(x0);
      }

      // $FF: synthetic method
      ColumnProperties(CodedInputStream x0, ExtensionRegistryLite x1, Object x2) throws InvalidProtocolBufferException {
         this(x0, x1);
      }

      public static final class Builder extends com.google.protobuf.GeneratedMessageV3.Builder<JdbcReaderProto.ColumnProperties.Builder> implements JdbcReaderProto.ColumnPropertiesOrBuilder {
         private int bitField0_;
         private Object columnName_;
         private List<JdbcReaderProto.ColumnProperty> properties_;
         private RepeatedFieldBuilderV3<JdbcReaderProto.ColumnProperty, JdbcReaderProto.ColumnProperty.Builder, JdbcReaderProto.ColumnPropertyOrBuilder> propertiesBuilder_;

         public static final Descriptor getDescriptor() {
            return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_descriptor;
         }

         protected FieldAccessorTable internalGetFieldAccessorTable() {
            return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_fieldAccessorTable.ensureFieldAccessorsInitialized(JdbcReaderProto.ColumnProperties.class, JdbcReaderProto.ColumnProperties.Builder.class);
         }

         private Builder() {
            this.columnName_ = "";
            this.properties_ = Collections.emptyList();
            this.maybeForceBuilderInitialization();
         }

         private Builder(BuilderParent parent) {
            super(parent);
            this.columnName_ = "";
            this.properties_ = Collections.emptyList();
            this.maybeForceBuilderInitialization();
         }

         private void maybeForceBuilderInitialization() {
            if (JdbcReaderProto.ColumnProperties.alwaysUseFieldBuilders) {
               this.getPropertiesFieldBuilder();
            }

         }

         public JdbcReaderProto.ColumnProperties.Builder clear() {
            super.clear();
            this.columnName_ = "";
            this.bitField0_ &= -2;
            if (this.propertiesBuilder_ == null) {
               this.properties_ = Collections.emptyList();
               this.bitField0_ &= -3;
            } else {
               this.propertiesBuilder_.clear();
            }

            return this;
         }

         public Descriptor getDescriptorForType() {
            return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperties_descriptor;
         }

         public JdbcReaderProto.ColumnProperties getDefaultInstanceForType() {
            return JdbcReaderProto.ColumnProperties.getDefaultInstance();
         }

         public JdbcReaderProto.ColumnProperties build() {
            JdbcReaderProto.ColumnProperties result = this.buildPartial();
            if (!result.isInitialized()) {
               throw newUninitializedMessageException(result);
            } else {
               return result;
            }
         }

         public JdbcReaderProto.ColumnProperties buildPartial() {
            JdbcReaderProto.ColumnProperties result = new JdbcReaderProto.ColumnProperties(this);
            int from_bitField0_ = this.bitField0_;
            int to_bitField0_ = 0;
            if ((from_bitField0_ & 1) != 0) {
               to_bitField0_ |= 1;
            }

            result.columnName_ = this.columnName_;
            if (this.propertiesBuilder_ == null) {
               if ((this.bitField0_ & 2) != 0) {
                  this.properties_ = Collections.unmodifiableList(this.properties_);
                  this.bitField0_ &= -3;
               }

               result.properties_ = this.properties_;
            } else {
               result.properties_ = this.propertiesBuilder_.build();
            }

            result.bitField0_ = to_bitField0_;
            this.onBuilt();
            return result;
         }

         public JdbcReaderProto.ColumnProperties.Builder clone() {
            return (JdbcReaderProto.ColumnProperties.Builder)super.clone();
         }

         public JdbcReaderProto.ColumnProperties.Builder setField(FieldDescriptor field, Object value) {
            return (JdbcReaderProto.ColumnProperties.Builder)super.setField(field, value);
         }

         public JdbcReaderProto.ColumnProperties.Builder clearField(FieldDescriptor field) {
            return (JdbcReaderProto.ColumnProperties.Builder)super.clearField(field);
         }

         public JdbcReaderProto.ColumnProperties.Builder clearOneof(OneofDescriptor oneof) {
            return (JdbcReaderProto.ColumnProperties.Builder)super.clearOneof(oneof);
         }

         public JdbcReaderProto.ColumnProperties.Builder setRepeatedField(FieldDescriptor field, int index, Object value) {
            return (JdbcReaderProto.ColumnProperties.Builder)super.setRepeatedField(field, index, value);
         }

         public JdbcReaderProto.ColumnProperties.Builder addRepeatedField(FieldDescriptor field, Object value) {
            return (JdbcReaderProto.ColumnProperties.Builder)super.addRepeatedField(field, value);
         }

         public JdbcReaderProto.ColumnProperties.Builder mergeFrom(Message other) {
            if (other instanceof JdbcReaderProto.ColumnProperties) {
               return this.mergeFrom((JdbcReaderProto.ColumnProperties)other);
            } else {
               super.mergeFrom(other);
               return this;
            }
         }

         public JdbcReaderProto.ColumnProperties.Builder mergeFrom(JdbcReaderProto.ColumnProperties other) {
            if (other == JdbcReaderProto.ColumnProperties.getDefaultInstance()) {
               return this;
            } else {
               if (other.hasColumnName()) {
                  this.bitField0_ |= 1;
                  this.columnName_ = other.columnName_;
                  this.onChanged();
               }

               if (this.propertiesBuilder_ == null) {
                  if (!other.properties_.isEmpty()) {
                     if (this.properties_.isEmpty()) {
                        this.properties_ = other.properties_;
                        this.bitField0_ &= -3;
                     } else {
                        this.ensurePropertiesIsMutable();
                        this.properties_.addAll(other.properties_);
                     }

                     this.onChanged();
                  }
               } else if (!other.properties_.isEmpty()) {
                  if (this.propertiesBuilder_.isEmpty()) {
                     this.propertiesBuilder_.dispose();
                     this.propertiesBuilder_ = null;
                     this.properties_ = other.properties_;
                     this.bitField0_ &= -3;
                     this.propertiesBuilder_ = JdbcReaderProto.ColumnProperties.alwaysUseFieldBuilders ? this.getPropertiesFieldBuilder() : null;
                  } else {
                     this.propertiesBuilder_.addAllMessages(other.properties_);
                  }
               }

               this.mergeUnknownFields(other.unknownFields);
               this.onChanged();
               return this;
            }
         }

         public final boolean isInitialized() {
            return true;
         }

         public JdbcReaderProto.ColumnProperties.Builder mergeFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
            JdbcReaderProto.ColumnProperties parsedMessage = null;

            try {
               parsedMessage = (JdbcReaderProto.ColumnProperties)JdbcReaderProto.ColumnProperties.PARSER.parsePartialFrom(input, extensionRegistry);
            } catch (InvalidProtocolBufferException var8) {
               parsedMessage = (JdbcReaderProto.ColumnProperties)var8.getUnfinishedMessage();
               throw var8.unwrapIOException();
            } finally {
               if (parsedMessage != null) {
                  this.mergeFrom(parsedMessage);
               }

            }

            return this;
         }

         public boolean hasColumnName() {
            return (this.bitField0_ & 1) != 0;
         }

         public String getColumnName() {
            Object ref = this.columnName_;
            if (!(ref instanceof String)) {
               ByteString bs = (ByteString)ref;
               String s = bs.toStringUtf8();
               if (bs.isValidUtf8()) {
                  this.columnName_ = s;
               }

               return s;
            } else {
               return (String)ref;
            }
         }

         public ByteString getColumnNameBytes() {
            Object ref = this.columnName_;
            if (ref instanceof String) {
               ByteString b = ByteString.copyFromUtf8((String)ref);
               this.columnName_ = b;
               return b;
            } else {
               return (ByteString)ref;
            }
         }

         public JdbcReaderProto.ColumnProperties.Builder setColumnName(String value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.bitField0_ |= 1;
               this.columnName_ = value;
               this.onChanged();
               return this;
            }
         }

         public JdbcReaderProto.ColumnProperties.Builder clearColumnName() {
            this.bitField0_ &= -2;
            this.columnName_ = JdbcReaderProto.ColumnProperties.getDefaultInstance().getColumnName();
            this.onChanged();
            return this;
         }

         public JdbcReaderProto.ColumnProperties.Builder setColumnNameBytes(ByteString value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.bitField0_ |= 1;
               this.columnName_ = value;
               this.onChanged();
               return this;
            }
         }

         private void ensurePropertiesIsMutable() {
            if ((this.bitField0_ & 2) == 0) {
               this.properties_ = new ArrayList(this.properties_);
               this.bitField0_ |= 2;
            }

         }

         public List<JdbcReaderProto.ColumnProperty> getPropertiesList() {
            return this.propertiesBuilder_ == null ? Collections.unmodifiableList(this.properties_) : this.propertiesBuilder_.getMessageList();
         }

         public int getPropertiesCount() {
            return this.propertiesBuilder_ == null ? this.properties_.size() : this.propertiesBuilder_.getCount();
         }

         public JdbcReaderProto.ColumnProperty getProperties(int index) {
            return this.propertiesBuilder_ == null ? (JdbcReaderProto.ColumnProperty)this.properties_.get(index) : (JdbcReaderProto.ColumnProperty)this.propertiesBuilder_.getMessage(index);
         }

         public JdbcReaderProto.ColumnProperties.Builder setProperties(int index, JdbcReaderProto.ColumnProperty value) {
            if (this.propertiesBuilder_ == null) {
               if (value == null) {
                  throw new NullPointerException();
               }

               this.ensurePropertiesIsMutable();
               this.properties_.set(index, value);
               this.onChanged();
            } else {
               this.propertiesBuilder_.setMessage(index, value);
            }

            return this;
         }

         public JdbcReaderProto.ColumnProperties.Builder setProperties(int index, JdbcReaderProto.ColumnProperty.Builder builderForValue) {
            if (this.propertiesBuilder_ == null) {
               this.ensurePropertiesIsMutable();
               this.properties_.set(index, builderForValue.build());
               this.onChanged();
            } else {
               this.propertiesBuilder_.setMessage(index, builderForValue.build());
            }

            return this;
         }

         public JdbcReaderProto.ColumnProperties.Builder addProperties(JdbcReaderProto.ColumnProperty value) {
            if (this.propertiesBuilder_ == null) {
               if (value == null) {
                  throw new NullPointerException();
               }

               this.ensurePropertiesIsMutable();
               this.properties_.add(value);
               this.onChanged();
            } else {
               this.propertiesBuilder_.addMessage(value);
            }

            return this;
         }

         public JdbcReaderProto.ColumnProperties.Builder addProperties(int index, JdbcReaderProto.ColumnProperty value) {
            if (this.propertiesBuilder_ == null) {
               if (value == null) {
                  throw new NullPointerException();
               }

               this.ensurePropertiesIsMutable();
               this.properties_.add(index, value);
               this.onChanged();
            } else {
               this.propertiesBuilder_.addMessage(index, value);
            }

            return this;
         }

         public JdbcReaderProto.ColumnProperties.Builder addProperties(JdbcReaderProto.ColumnProperty.Builder builderForValue) {
            if (this.propertiesBuilder_ == null) {
               this.ensurePropertiesIsMutable();
               this.properties_.add(builderForValue.build());
               this.onChanged();
            } else {
               this.propertiesBuilder_.addMessage(builderForValue.build());
            }

            return this;
         }

         public JdbcReaderProto.ColumnProperties.Builder addProperties(int index, JdbcReaderProto.ColumnProperty.Builder builderForValue) {
            if (this.propertiesBuilder_ == null) {
               this.ensurePropertiesIsMutable();
               this.properties_.add(index, builderForValue.build());
               this.onChanged();
            } else {
               this.propertiesBuilder_.addMessage(index, builderForValue.build());
            }

            return this;
         }

         public JdbcReaderProto.ColumnProperties.Builder addAllProperties(Iterable<? extends JdbcReaderProto.ColumnProperty> values) {
            if (this.propertiesBuilder_ == null) {
               this.ensurePropertiesIsMutable();
               com.google.protobuf.AbstractMessageLite.Builder.addAll(values, this.properties_);
               this.onChanged();
            } else {
               this.propertiesBuilder_.addAllMessages(values);
            }

            return this;
         }

         public JdbcReaderProto.ColumnProperties.Builder clearProperties() {
            if (this.propertiesBuilder_ == null) {
               this.properties_ = Collections.emptyList();
               this.bitField0_ &= -3;
               this.onChanged();
            } else {
               this.propertiesBuilder_.clear();
            }

            return this;
         }

         public JdbcReaderProto.ColumnProperties.Builder removeProperties(int index) {
            if (this.propertiesBuilder_ == null) {
               this.ensurePropertiesIsMutable();
               this.properties_.remove(index);
               this.onChanged();
            } else {
               this.propertiesBuilder_.remove(index);
            }

            return this;
         }

         public JdbcReaderProto.ColumnProperty.Builder getPropertiesBuilder(int index) {
            return (JdbcReaderProto.ColumnProperty.Builder)this.getPropertiesFieldBuilder().getBuilder(index);
         }

         public JdbcReaderProto.ColumnPropertyOrBuilder getPropertiesOrBuilder(int index) {
            return this.propertiesBuilder_ == null ? (JdbcReaderProto.ColumnPropertyOrBuilder)this.properties_.get(index) : (JdbcReaderProto.ColumnPropertyOrBuilder)this.propertiesBuilder_.getMessageOrBuilder(index);
         }

         public List<? extends JdbcReaderProto.ColumnPropertyOrBuilder> getPropertiesOrBuilderList() {
            return this.propertiesBuilder_ != null ? this.propertiesBuilder_.getMessageOrBuilderList() : Collections.unmodifiableList(this.properties_);
         }

         public JdbcReaderProto.ColumnProperty.Builder addPropertiesBuilder() {
            return (JdbcReaderProto.ColumnProperty.Builder)this.getPropertiesFieldBuilder().addBuilder(JdbcReaderProto.ColumnProperty.getDefaultInstance());
         }

         public JdbcReaderProto.ColumnProperty.Builder addPropertiesBuilder(int index) {
            return (JdbcReaderProto.ColumnProperty.Builder)this.getPropertiesFieldBuilder().addBuilder(index, JdbcReaderProto.ColumnProperty.getDefaultInstance());
         }

         public List<JdbcReaderProto.ColumnProperty.Builder> getPropertiesBuilderList() {
            return this.getPropertiesFieldBuilder().getBuilderList();
         }

         private RepeatedFieldBuilderV3<JdbcReaderProto.ColumnProperty, JdbcReaderProto.ColumnProperty.Builder, JdbcReaderProto.ColumnPropertyOrBuilder> getPropertiesFieldBuilder() {
            if (this.propertiesBuilder_ == null) {
               this.propertiesBuilder_ = new RepeatedFieldBuilderV3(this.properties_, (this.bitField0_ & 2) != 0, this.getParentForChildren(), this.isClean());
               this.properties_ = null;
            }

            return this.propertiesBuilder_;
         }

         public final JdbcReaderProto.ColumnProperties.Builder setUnknownFields(UnknownFieldSet unknownFields) {
            return (JdbcReaderProto.ColumnProperties.Builder)super.setUnknownFields(unknownFields);
         }

         public final JdbcReaderProto.ColumnProperties.Builder mergeUnknownFields(UnknownFieldSet unknownFields) {
            return (JdbcReaderProto.ColumnProperties.Builder)super.mergeUnknownFields(unknownFields);
         }

         // $FF: synthetic method
         Builder(Object x0) {
            this();
         }

         // $FF: synthetic method
         Builder(BuilderParent x0, Object x1) {
            this(x0);
         }
      }
   }

   public interface ColumnPropertiesOrBuilder extends MessageOrBuilder {
      boolean hasColumnName();

      String getColumnName();

      ByteString getColumnNameBytes();

      List<JdbcReaderProto.ColumnProperty> getPropertiesList();

      JdbcReaderProto.ColumnProperty getProperties(int var1);

      int getPropertiesCount();

      List<? extends JdbcReaderProto.ColumnPropertyOrBuilder> getPropertiesOrBuilderList();

      JdbcReaderProto.ColumnPropertyOrBuilder getPropertiesOrBuilder(int var1);
   }

   public static final class ColumnProperty extends GeneratedMessageV3 implements JdbcReaderProto.ColumnPropertyOrBuilder {
      private static final long serialVersionUID = 0L;
      private int bitField0_;
      public static final int KEY_FIELD_NUMBER = 1;
      private volatile Object key_;
      public static final int VALUE_FIELD_NUMBER = 2;
      private volatile Object value_;
      private byte memoizedIsInitialized;
      private static final JdbcReaderProto.ColumnProperty DEFAULT_INSTANCE = new JdbcReaderProto.ColumnProperty();
      /** @deprecated */
      @Deprecated
      public static final Parser<JdbcReaderProto.ColumnProperty> PARSER = new AbstractParser<JdbcReaderProto.ColumnProperty>() {
         public JdbcReaderProto.ColumnProperty parsePartialFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
            return new JdbcReaderProto.ColumnProperty(input, extensionRegistry);
         }
      };

      private ColumnProperty(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
         super(builder);
         this.memoizedIsInitialized = -1;
      }

      private ColumnProperty() {
         this.memoizedIsInitialized = -1;
         this.key_ = "";
         this.value_ = "";
      }

      protected Object newInstance(UnusedPrivateParameter unused) {
         return new JdbcReaderProto.ColumnProperty();
      }

      public final UnknownFieldSet getUnknownFields() {
         return this.unknownFields;
      }

      private ColumnProperty(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         this();
         if (extensionRegistry == null) {
            throw new NullPointerException();
         } else {
            int mutable_bitField0_ = false;
            com.google.protobuf.UnknownFieldSet.Builder unknownFields = UnknownFieldSet.newBuilder();

            try {
               boolean done = false;

               while(!done) {
                  int tag = input.readTag();
                  ByteString bs;
                  switch(tag) {
                  case 0:
                     done = true;
                     break;
                  case 10:
                     bs = input.readBytes();
                     this.bitField0_ |= 1;
                     this.key_ = bs;
                     break;
                  case 18:
                     bs = input.readBytes();
                     this.bitField0_ |= 2;
                     this.value_ = bs;
                     break;
                  default:
                     if (!this.parseUnknownField(input, unknownFields, extensionRegistry, tag)) {
                        done = true;
                     }
                  }
               }
            } catch (InvalidProtocolBufferException var12) {
               throw var12.setUnfinishedMessage(this);
            } catch (IOException var13) {
               throw (new InvalidProtocolBufferException(var13)).setUnfinishedMessage(this);
            } finally {
               this.unknownFields = unknownFields.build();
               this.makeExtensionsImmutable();
            }

         }
      }

      public static final Descriptor getDescriptor() {
         return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_descriptor;
      }

      protected FieldAccessorTable internalGetFieldAccessorTable() {
         return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_fieldAccessorTable.ensureFieldAccessorsInitialized(JdbcReaderProto.ColumnProperty.class, JdbcReaderProto.ColumnProperty.Builder.class);
      }

      public boolean hasKey() {
         return (this.bitField0_ & 1) != 0;
      }

      public String getKey() {
         Object ref = this.key_;
         if (ref instanceof String) {
            return (String)ref;
         } else {
            ByteString bs = (ByteString)ref;
            String s = bs.toStringUtf8();
            if (bs.isValidUtf8()) {
               this.key_ = s;
            }

            return s;
         }
      }

      public ByteString getKeyBytes() {
         Object ref = this.key_;
         if (ref instanceof String) {
            ByteString b = ByteString.copyFromUtf8((String)ref);
            this.key_ = b;
            return b;
         } else {
            return (ByteString)ref;
         }
      }

      public boolean hasValue() {
         return (this.bitField0_ & 2) != 0;
      }

      public String getValue() {
         Object ref = this.value_;
         if (ref instanceof String) {
            return (String)ref;
         } else {
            ByteString bs = (ByteString)ref;
            String s = bs.toStringUtf8();
            if (bs.isValidUtf8()) {
               this.value_ = s;
            }

            return s;
         }
      }

      public ByteString getValueBytes() {
         Object ref = this.value_;
         if (ref instanceof String) {
            ByteString b = ByteString.copyFromUtf8((String)ref);
            this.value_ = b;
            return b;
         } else {
            return (ByteString)ref;
         }
      }

      public final boolean isInitialized() {
         byte isInitialized = this.memoizedIsInitialized;
         if (isInitialized == 1) {
            return true;
         } else if (isInitialized == 0) {
            return false;
         } else {
            this.memoizedIsInitialized = 1;
            return true;
         }
      }

      public void writeTo(CodedOutputStream output) throws IOException {
         if ((this.bitField0_ & 1) != 0) {
            GeneratedMessageV3.writeString(output, 1, this.key_);
         }

         if ((this.bitField0_ & 2) != 0) {
            GeneratedMessageV3.writeString(output, 2, this.value_);
         }

         this.unknownFields.writeTo(output);
      }

      public int getSerializedSize() {
         int size = this.memoizedSize;
         if (size != -1) {
            return size;
         } else {
            size = 0;
            if ((this.bitField0_ & 1) != 0) {
               size += GeneratedMessageV3.computeStringSize(1, this.key_);
            }

            if ((this.bitField0_ & 2) != 0) {
               size += GeneratedMessageV3.computeStringSize(2, this.value_);
            }

            size += this.unknownFields.getSerializedSize();
            this.memoizedSize = size;
            return size;
         }
      }

      public boolean equals(Object obj) {
         if (obj == this) {
            return true;
         } else if (!(obj instanceof JdbcReaderProto.ColumnProperty)) {
            return super.equals(obj);
         } else {
            JdbcReaderProto.ColumnProperty other = (JdbcReaderProto.ColumnProperty)obj;
            if (this.hasKey() != other.hasKey()) {
               return false;
            } else if (this.hasKey() && !this.getKey().equals(other.getKey())) {
               return false;
            } else if (this.hasValue() != other.hasValue()) {
               return false;
            } else if (this.hasValue() && !this.getValue().equals(other.getValue())) {
               return false;
            } else {
               return this.unknownFields.equals(other.unknownFields);
            }
         }
      }

      public int hashCode() {
         if (this.memoizedHashCode != 0) {
            return this.memoizedHashCode;
         } else {
            int hash = 41;
            int hash = 19 * hash + getDescriptor().hashCode();
            if (this.hasKey()) {
               hash = 37 * hash + 1;
               hash = 53 * hash + this.getKey().hashCode();
            }

            if (this.hasValue()) {
               hash = 37 * hash + 2;
               hash = 53 * hash + this.getValue().hashCode();
            }

            hash = 29 * hash + this.unknownFields.hashCode();
            this.memoizedHashCode = hash;
            return hash;
         }
      }

      public static JdbcReaderProto.ColumnProperty parseFrom(ByteBuffer data) throws InvalidProtocolBufferException {
         return (JdbcReaderProto.ColumnProperty)PARSER.parseFrom(data);
      }

      public static JdbcReaderProto.ColumnProperty parseFrom(ByteBuffer data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         return (JdbcReaderProto.ColumnProperty)PARSER.parseFrom(data, extensionRegistry);
      }

      public static JdbcReaderProto.ColumnProperty parseFrom(ByteString data) throws InvalidProtocolBufferException {
         return (JdbcReaderProto.ColumnProperty)PARSER.parseFrom(data);
      }

      public static JdbcReaderProto.ColumnProperty parseFrom(ByteString data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         return (JdbcReaderProto.ColumnProperty)PARSER.parseFrom(data, extensionRegistry);
      }

      public static JdbcReaderProto.ColumnProperty parseFrom(byte[] data) throws InvalidProtocolBufferException {
         return (JdbcReaderProto.ColumnProperty)PARSER.parseFrom(data);
      }

      public static JdbcReaderProto.ColumnProperty parseFrom(byte[] data, ExtensionRegistryLite extensionRegistry) throws InvalidProtocolBufferException {
         return (JdbcReaderProto.ColumnProperty)PARSER.parseFrom(data, extensionRegistry);
      }

      public static JdbcReaderProto.ColumnProperty parseFrom(InputStream input) throws IOException {
         return (JdbcReaderProto.ColumnProperty)GeneratedMessageV3.parseWithIOException(PARSER, input);
      }

      public static JdbcReaderProto.ColumnProperty parseFrom(InputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
         return (JdbcReaderProto.ColumnProperty)GeneratedMessageV3.parseWithIOException(PARSER, input, extensionRegistry);
      }

      public static JdbcReaderProto.ColumnProperty parseDelimitedFrom(InputStream input) throws IOException {
         return (JdbcReaderProto.ColumnProperty)GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input);
      }

      public static JdbcReaderProto.ColumnProperty parseDelimitedFrom(InputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
         return (JdbcReaderProto.ColumnProperty)GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input, extensionRegistry);
      }

      public static JdbcReaderProto.ColumnProperty parseFrom(CodedInputStream input) throws IOException {
         return (JdbcReaderProto.ColumnProperty)GeneratedMessageV3.parseWithIOException(PARSER, input);
      }

      public static JdbcReaderProto.ColumnProperty parseFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
         return (JdbcReaderProto.ColumnProperty)GeneratedMessageV3.parseWithIOException(PARSER, input, extensionRegistry);
      }

      public JdbcReaderProto.ColumnProperty.Builder newBuilderForType() {
         return newBuilder();
      }

      public static JdbcReaderProto.ColumnProperty.Builder newBuilder() {
         return DEFAULT_INSTANCE.toBuilder();
      }

      public static JdbcReaderProto.ColumnProperty.Builder newBuilder(JdbcReaderProto.ColumnProperty prototype) {
         return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
      }

      public JdbcReaderProto.ColumnProperty.Builder toBuilder() {
         return this == DEFAULT_INSTANCE ? new JdbcReaderProto.ColumnProperty.Builder() : (new JdbcReaderProto.ColumnProperty.Builder()).mergeFrom(this);
      }

      protected JdbcReaderProto.ColumnProperty.Builder newBuilderForType(BuilderParent parent) {
         JdbcReaderProto.ColumnProperty.Builder builder = new JdbcReaderProto.ColumnProperty.Builder(parent);
         return builder;
      }

      public static JdbcReaderProto.ColumnProperty getDefaultInstance() {
         return DEFAULT_INSTANCE;
      }

      public static Parser<JdbcReaderProto.ColumnProperty> parser() {
         return PARSER;
      }

      public Parser<JdbcReaderProto.ColumnProperty> getParserForType() {
         return PARSER;
      }

      public JdbcReaderProto.ColumnProperty getDefaultInstanceForType() {
         return DEFAULT_INSTANCE;
      }

      // $FF: synthetic method
      ColumnProperty(com.google.protobuf.GeneratedMessageV3.Builder x0, Object x1) {
         this(x0);
      }

      // $FF: synthetic method
      ColumnProperty(CodedInputStream x0, ExtensionRegistryLite x1, Object x2) throws InvalidProtocolBufferException {
         this(x0, x1);
      }

      public static final class Builder extends com.google.protobuf.GeneratedMessageV3.Builder<JdbcReaderProto.ColumnProperty.Builder> implements JdbcReaderProto.ColumnPropertyOrBuilder {
         private int bitField0_;
         private Object key_;
         private Object value_;

         public static final Descriptor getDescriptor() {
            return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_descriptor;
         }

         protected FieldAccessorTable internalGetFieldAccessorTable() {
            return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_fieldAccessorTable.ensureFieldAccessorsInitialized(JdbcReaderProto.ColumnProperty.class, JdbcReaderProto.ColumnProperty.Builder.class);
         }

         private Builder() {
            this.key_ = "";
            this.value_ = "";
            this.maybeForceBuilderInitialization();
         }

         private Builder(BuilderParent parent) {
            super(parent);
            this.key_ = "";
            this.value_ = "";
            this.maybeForceBuilderInitialization();
         }

         private void maybeForceBuilderInitialization() {
            if (JdbcReaderProto.ColumnProperty.alwaysUseFieldBuilders) {
            }

         }

         public JdbcReaderProto.ColumnProperty.Builder clear() {
            super.clear();
            this.key_ = "";
            this.bitField0_ &= -2;
            this.value_ = "";
            this.bitField0_ &= -3;
            return this;
         }

         public Descriptor getDescriptorForType() {
            return JdbcReaderProto.internal_static_com_dremio_exec_store_jdbc_proto_ColumnProperty_descriptor;
         }

         public JdbcReaderProto.ColumnProperty getDefaultInstanceForType() {
            return JdbcReaderProto.ColumnProperty.getDefaultInstance();
         }

         public JdbcReaderProto.ColumnProperty build() {
            JdbcReaderProto.ColumnProperty result = this.buildPartial();
            if (!result.isInitialized()) {
               throw newUninitializedMessageException(result);
            } else {
               return result;
            }
         }

         public JdbcReaderProto.ColumnProperty buildPartial() {
            JdbcReaderProto.ColumnProperty result = new JdbcReaderProto.ColumnProperty(this);
            int from_bitField0_ = this.bitField0_;
            int to_bitField0_ = 0;
            if ((from_bitField0_ & 1) != 0) {
               to_bitField0_ |= 1;
            }

            result.key_ = this.key_;
            if ((from_bitField0_ & 2) != 0) {
               to_bitField0_ |= 2;
            }

            result.value_ = this.value_;
            result.bitField0_ = to_bitField0_;
            this.onBuilt();
            return result;
         }

         public JdbcReaderProto.ColumnProperty.Builder clone() {
            return (JdbcReaderProto.ColumnProperty.Builder)super.clone();
         }

         public JdbcReaderProto.ColumnProperty.Builder setField(FieldDescriptor field, Object value) {
            return (JdbcReaderProto.ColumnProperty.Builder)super.setField(field, value);
         }

         public JdbcReaderProto.ColumnProperty.Builder clearField(FieldDescriptor field) {
            return (JdbcReaderProto.ColumnProperty.Builder)super.clearField(field);
         }

         public JdbcReaderProto.ColumnProperty.Builder clearOneof(OneofDescriptor oneof) {
            return (JdbcReaderProto.ColumnProperty.Builder)super.clearOneof(oneof);
         }

         public JdbcReaderProto.ColumnProperty.Builder setRepeatedField(FieldDescriptor field, int index, Object value) {
            return (JdbcReaderProto.ColumnProperty.Builder)super.setRepeatedField(field, index, value);
         }

         public JdbcReaderProto.ColumnProperty.Builder addRepeatedField(FieldDescriptor field, Object value) {
            return (JdbcReaderProto.ColumnProperty.Builder)super.addRepeatedField(field, value);
         }

         public JdbcReaderProto.ColumnProperty.Builder mergeFrom(Message other) {
            if (other instanceof JdbcReaderProto.ColumnProperty) {
               return this.mergeFrom((JdbcReaderProto.ColumnProperty)other);
            } else {
               super.mergeFrom(other);
               return this;
            }
         }

         public JdbcReaderProto.ColumnProperty.Builder mergeFrom(JdbcReaderProto.ColumnProperty other) {
            if (other == JdbcReaderProto.ColumnProperty.getDefaultInstance()) {
               return this;
            } else {
               if (other.hasKey()) {
                  this.bitField0_ |= 1;
                  this.key_ = other.key_;
                  this.onChanged();
               }

               if (other.hasValue()) {
                  this.bitField0_ |= 2;
                  this.value_ = other.value_;
                  this.onChanged();
               }

               this.mergeUnknownFields(other.unknownFields);
               this.onChanged();
               return this;
            }
         }

         public final boolean isInitialized() {
            return true;
         }

         public JdbcReaderProto.ColumnProperty.Builder mergeFrom(CodedInputStream input, ExtensionRegistryLite extensionRegistry) throws IOException {
            JdbcReaderProto.ColumnProperty parsedMessage = null;

            try {
               parsedMessage = (JdbcReaderProto.ColumnProperty)JdbcReaderProto.ColumnProperty.PARSER.parsePartialFrom(input, extensionRegistry);
            } catch (InvalidProtocolBufferException var8) {
               parsedMessage = (JdbcReaderProto.ColumnProperty)var8.getUnfinishedMessage();
               throw var8.unwrapIOException();
            } finally {
               if (parsedMessage != null) {
                  this.mergeFrom(parsedMessage);
               }

            }

            return this;
         }

         public boolean hasKey() {
            return (this.bitField0_ & 1) != 0;
         }

         public String getKey() {
            Object ref = this.key_;
            if (!(ref instanceof String)) {
               ByteString bs = (ByteString)ref;
               String s = bs.toStringUtf8();
               if (bs.isValidUtf8()) {
                  this.key_ = s;
               }

               return s;
            } else {
               return (String)ref;
            }
         }

         public ByteString getKeyBytes() {
            Object ref = this.key_;
            if (ref instanceof String) {
               ByteString b = ByteString.copyFromUtf8((String)ref);
               this.key_ = b;
               return b;
            } else {
               return (ByteString)ref;
            }
         }

         public JdbcReaderProto.ColumnProperty.Builder setKey(String value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.bitField0_ |= 1;
               this.key_ = value;
               this.onChanged();
               return this;
            }
         }

         public JdbcReaderProto.ColumnProperty.Builder clearKey() {
            this.bitField0_ &= -2;
            this.key_ = JdbcReaderProto.ColumnProperty.getDefaultInstance().getKey();
            this.onChanged();
            return this;
         }

         public JdbcReaderProto.ColumnProperty.Builder setKeyBytes(ByteString value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.bitField0_ |= 1;
               this.key_ = value;
               this.onChanged();
               return this;
            }
         }

         public boolean hasValue() {
            return (this.bitField0_ & 2) != 0;
         }

         public String getValue() {
            Object ref = this.value_;
            if (!(ref instanceof String)) {
               ByteString bs = (ByteString)ref;
               String s = bs.toStringUtf8();
               if (bs.isValidUtf8()) {
                  this.value_ = s;
               }

               return s;
            } else {
               return (String)ref;
            }
         }

         public ByteString getValueBytes() {
            Object ref = this.value_;
            if (ref instanceof String) {
               ByteString b = ByteString.copyFromUtf8((String)ref);
               this.value_ = b;
               return b;
            } else {
               return (ByteString)ref;
            }
         }

         public JdbcReaderProto.ColumnProperty.Builder setValue(String value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.bitField0_ |= 2;
               this.value_ = value;
               this.onChanged();
               return this;
            }
         }

         public JdbcReaderProto.ColumnProperty.Builder clearValue() {
            this.bitField0_ &= -3;
            this.value_ = JdbcReaderProto.ColumnProperty.getDefaultInstance().getValue();
            this.onChanged();
            return this;
         }

         public JdbcReaderProto.ColumnProperty.Builder setValueBytes(ByteString value) {
            if (value == null) {
               throw new NullPointerException();
            } else {
               this.bitField0_ |= 2;
               this.value_ = value;
               this.onChanged();
               return this;
            }
         }

         public final JdbcReaderProto.ColumnProperty.Builder setUnknownFields(UnknownFieldSet unknownFields) {
            return (JdbcReaderProto.ColumnProperty.Builder)super.setUnknownFields(unknownFields);
         }

         public final JdbcReaderProto.ColumnProperty.Builder mergeUnknownFields(UnknownFieldSet unknownFields) {
            return (JdbcReaderProto.ColumnProperty.Builder)super.mergeUnknownFields(unknownFields);
         }

         // $FF: synthetic method
         Builder(Object x0) {
            this();
         }

         // $FF: synthetic method
         Builder(BuilderParent x0, Object x1) {
            this(x0);
         }
      }
   }

   public interface ColumnPropertyOrBuilder extends MessageOrBuilder {
      boolean hasKey();

      String getKey();

      ByteString getKeyBytes();

      boolean hasValue();

      String getValue();

      ByteString getValueBytes();
   }
}
