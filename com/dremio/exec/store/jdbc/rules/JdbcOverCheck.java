package com.dremio.exec.store.jdbc.rules;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.store.jdbc.conf.DialectConf;
import com.dremio.exec.store.jdbc.dialect.JdbcDremioSqlDialect;
import com.google.common.collect.UnmodifiableIterator;
import java.util.Iterator;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcOverCheck implements RexVisitor<Boolean> {
   private static final Logger logger = LoggerFactory.getLogger(JdbcOverCheck.class);
   private final JdbcDremioSqlDialect dialect;

   public static boolean hasOver(RexNode rex, StoragePluginId pluginId) {
      DialectConf<?, ?> conf = (DialectConf)pluginId.getConnectionConf();
      return (Boolean)rex.accept(new JdbcOverCheck(conf.getDialect()));
   }

   private JdbcOverCheck(JdbcDremioSqlDialect dialect) {
      this.dialect = dialect;
   }

   public Boolean visitInputRef(RexInputRef paramRexInputRef) {
      return true;
   }

   public Boolean visitLocalRef(RexLocalRef paramRexLocalRef) {
      return true;
   }

   public Boolean visitPatternFieldRef(RexPatternFieldRef fieldRef) {
      return true;
   }

   public Boolean visitTableInputRef(RexTableInputRef fieldRef) {
      return true;
   }

   public Boolean visitLiteral(RexLiteral paramRexLiteral) {
      return true;
   }

   public Boolean visitCall(RexCall paramRexCall) {
      UnmodifiableIterator var2 = paramRexCall.operands.iterator();

      RexNode operand;
      do {
         if (!var2.hasNext()) {
            return true;
         }

         operand = (RexNode)var2.next();
      } while((Boolean)operand.accept(this));

      return false;
   }

   public Boolean visitOver(RexOver over) {
      logger.debug("Evaluating if Over clause is supported using supportsOver: {}.", over);
      boolean overSupported = this.dialect.supportsOver(over);
      if (!overSupported) {
         logger.debug("Over clause was not supported. Aborting pushdown.");
         return false;
      } else {
         logger.debug("Over clause was supported.");
         return true;
      }
   }

   public Boolean visitCorrelVariable(RexCorrelVariable paramRexCorrelVariable) {
      return true;
   }

   public Boolean visitDynamicParam(RexDynamicParam paramRexDynamicParam) {
      return true;
   }

   public Boolean visitRangeRef(RexRangeRef paramRexRangeRef) {
      return true;
   }

   public Boolean visitFieldAccess(RexFieldAccess paramRexFieldAccess) {
      return (Boolean)paramRexFieldAccess.getReferenceExpr().accept(this);
   }

   public Boolean visitSubQuery(RexSubQuery subQuery) {
      Iterator var2 = subQuery.getOperands().iterator();

      RexNode operand;
      do {
         if (!var2.hasNext()) {
            return true;
         }

         operand = (RexNode)var2.next();
      } while((Boolean)operand.accept(this));

      return false;
   }
}
