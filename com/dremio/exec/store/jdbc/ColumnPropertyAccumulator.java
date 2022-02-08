package com.dremio.exec.store.jdbc;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.store.jdbc.proto.JdbcReaderProto;
import com.dremio.exec.store.jdbc.rel.JdbcIntermediatePrel;
import com.dremio.exec.store.jdbc.rel.JdbcTableScan;
import com.dremio.exec.store.jdbc.rules.UnpushableTypeVisitor;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnPropertyAccumulator extends StatelessRelShuttleImpl {
   private static final Logger logger = LoggerFactory.getLogger(UnpushableTypeVisitor.class);
   private final Map<String, Map<String, String>> columnProperties = new HashMap();

   public RelNode visit(TableScan scan) {
      JdbcTableScan tableScan = (JdbcTableScan)scan;
      ReadDefinition readDefinition = tableScan.getTableMetadata().getReadDefinition();
      if (readDefinition.getExtendedProperty() != null) {
         try {
            JdbcReaderProto.JdbcTableXattr attrs = JdbcReaderProto.JdbcTableXattr.parseFrom(readDefinition.getExtendedProperty().asReadOnlyByteBuffer());
            Iterator var5 = attrs.getColumnPropertiesList().iterator();

            while(var5.hasNext()) {
               JdbcReaderProto.ColumnProperties colProps = (JdbcReaderProto.ColumnProperties)var5.next();
               Map<String, String> properties = new HashMap();
               Iterator var8 = colProps.getPropertiesList().iterator();

               while(var8.hasNext()) {
                  JdbcReaderProto.ColumnProperty colProp = (JdbcReaderProto.ColumnProperty)var8.next();
                  properties.put(colProp.getKey(), colProp.getValue());
               }

               this.columnProperties.put(colProps.getColumnName(), properties);
            }
         } catch (InvalidProtocolBufferException var10) {
            logger.warn("Unable to get extended properties for table {}.", tableScan.getTableName(), var10);
         }
      }

      return super.visit(scan);
   }

   public RelNode visit(RelNode other) {
      if (other instanceof HepRelVertex) {
         other = ((HepRelVertex)other).getCurrentRel();
      }

      if (other instanceof JdbcTableScan) {
         return this.visit((TableScan)((JdbcTableScan)other));
      } else {
         return other instanceof JdbcIntermediatePrel ? this.visit(((JdbcIntermediatePrel)other).getSubTree()) : super.visit(other);
      }
   }

   public Map<String, Map<String, String>> getColumnProperties() {
      return this.columnProperties;
   }
}
