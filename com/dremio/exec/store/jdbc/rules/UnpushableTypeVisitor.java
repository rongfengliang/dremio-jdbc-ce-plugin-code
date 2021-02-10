package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.store.jdbc.ColumnPropertyAccumulator;
import com.dremio.exec.store.jdbc.rel.JdbcAggregate;
import com.dremio.exec.store.jdbc.rel.JdbcJoin;
import com.dremio.exec.store.jdbc.rel.JdbcProject;
import com.dremio.exec.store.jdbc.rel.JdbcTableScan;
import com.dremio.exec.store.jdbc.rel.JdbcWindow;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.util.Util.FoundOne;

public final class UnpushableTypeVisitor extends RexVisitorImpl<Boolean> {
   private final Map<String, Map<String, String>> columnProperties;
   private final RelNode rootNode;

   private UnpushableTypeVisitor(RelNode node, Map<String, Map<String, String>> columnProperties) {
      super(true);
      this.rootNode = node;
      this.columnProperties = columnProperties;
   }

   public static boolean hasUnpushableTypes(RelNode node, RexNode rexNode) {
      return hasUnpushableTypes(node, (List)ImmutableList.of(rexNode));
   }

   public static boolean hasUnpushableTypes(RelNode node, List<RexNode> rexNodes) {
      return hasUnpushableTypes(node, rexNodes, () -> {
         ColumnPropertyAccumulator accumulator = new ColumnPropertyAccumulator();
         node.accept(accumulator);
         return accumulator.getColumnProperties();
      });
   }

   private static boolean hasUnpushableTypes(RelNode node, List<RexNode> rexNodes, Supplier<Map<String, Map<String, String>>> propertyProducer) {
      UnpushableTypeVisitor visitor = null;
      Iterator itr = rexNodes.stream().filter((n) -> {
         return !(n instanceof RexInputRef);
      }).iterator();

      try {
         do {
            if (!itr.hasNext()) {
               return false;
            }

            if (null == visitor) {
               visitor = new UnpushableTypeVisitor(node, (Map)propertyProducer.get());
            }
         } while(!(Boolean)((RexNode)itr.next()).accept(visitor));

         return true;
      } catch (FoundOne var6) {
         return (Boolean)var6.getNode();
      }
   }

   public Boolean visitInputRef(RexInputRef inputRef) {
      int refIndex = inputRef.getIndex();
      UnpushableTypeVisitor.UnpushableRelVisitor finder = new UnpushableTypeVisitor.UnpushableRelVisitor(refIndex, this.columnProperties);
      finder.go(this.rootNode);
      if (finder.hasUnpushableTypes()) {
         throw new FoundOne(Boolean.TRUE);
      } else {
         return false;
      }
   }

   public Boolean visitOver(RexOver over) {
      RexWindow window = over.getWindow();
      UnmodifiableIterator var3 = window.orderKeys.iterator();

      RexFieldCollation orderKey;
      do {
         if (!var3.hasNext()) {
            var3 = window.partitionKeys.iterator();

            RexNode partitionKey;
            do {
               if (!var3.hasNext()) {
                  return false;
               }

               partitionKey = (RexNode)var3.next();
            } while(!(Boolean)partitionKey.accept(this));

            throw new FoundOne(Boolean.TRUE);
         }

         orderKey = (RexFieldCollation)var3.next();
      } while(!(Boolean)((RexNode)orderKey.left).accept(this));

      throw new FoundOne(Boolean.TRUE);
   }

   public Boolean visitLocalRef(RexLocalRef localRef) {
      return hasUnpushableType(localRef.getName(), this.columnProperties);
   }

   public Boolean visitLiteral(RexLiteral literal) {
      return false;
   }

   public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
      return false;
   }

   public Boolean visitCall(RexCall call) {
      if (this.deep) {
         Iterator var2 = call.getOperands().iterator();

         while(var2.hasNext()) {
            RexNode node = (RexNode)var2.next();
            if ((Boolean)node.accept(this)) {
               throw new FoundOne(Boolean.TRUE);
            }
         }
      }

      return false;
   }

   public Boolean visitDynamicParam(RexDynamicParam dynamicParam) {
      return false;
   }

   public Boolean visitPatternFieldRef(RexPatternFieldRef fieldRef) {
      return false;
   }

   public Boolean visitRangeRef(RexRangeRef rangeRef) {
      return false;
   }

   public Boolean visitSubQuery(RexSubQuery subQuery) {
      UnpushableTypeVisitor visitor = new UnpushableTypeVisitor(subQuery.rel, this.columnProperties);
      Iterator var3 = subQuery.rel.getChildExps().iterator();

      RexNode node;
      do {
         if (!var3.hasNext()) {
            return false;
         }

         node = (RexNode)var3.next();
      } while(!(Boolean)node.accept(visitor));

      throw new FoundOne(Boolean.TRUE);
   }

   public static Boolean hasUnpushableType(String name, Map<String, Map<String, String>> columnProperties) {
      Map<String, String> colProperties = (Map)columnProperties.get(name.toLowerCase(Locale.ROOT));
      return null != colProperties ? Boolean.parseBoolean((String)colProperties.get("unpushable")) : false;
   }

   private static class UnpushableRelVisitor extends RelVisitor {
      private final Map<String, Map<String, String>> columnProperties;
      private int curColumnIndex;
      private boolean hasUnpushableTypes;

      UnpushableRelVisitor(int index, Map<String, Map<String, String>> columnProperties) {
         this.curColumnIndex = index;
         this.columnProperties = columnProperties;
         this.hasUnpushableTypes = false;
      }

      public void visit(RelNode node, int ordinal, RelNode parent) {
         if (node instanceof HepRelVertex) {
            node = ((HepRelVertex)node).getCurrentRel();
         }

         if (node instanceof JdbcJoin) {
            JdbcJoin join = (JdbcJoin)node;
            if (join.getLeft().getRowType().getFieldCount() > this.curColumnIndex) {
               this.visit(join.getLeft(), ordinal, node);
            } else {
               this.curColumnIndex -= join.getLeft().getRowType().getFieldCount();
               this.visit(join.getRight(), ordinal, node);
            }
         } else if (!(node instanceof JdbcProject) && !(node instanceof JdbcAggregate)) {
            if (!(node instanceof JdbcTableScan) && !(node instanceof JdbcWindow)) {
               super.visit(node, ordinal, parent);
            } else {
               String colName = (String)node.getRowType().getFieldNames().get(this.curColumnIndex);
               this.hasUnpushableTypes |= UnpushableTypeVisitor.hasUnpushableType(colName, this.columnProperties);
            }
         } else {
            SingleRel singleRel = (SingleRel)node;
            this.hasUnpushableTypes |= UnpushableTypeVisitor.hasUnpushableTypes(singleRel.getInput(), singleRel.getChildExps(), () -> {
               return this.columnProperties;
            });
         }

      }

      public Boolean hasUnpushableTypes() {
         return this.hasUnpushableTypes;
      }
   }
}
