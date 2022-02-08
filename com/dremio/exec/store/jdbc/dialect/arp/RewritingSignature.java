package com.dremio.exec.store.jdbc.dialect.arp;

import com.dremio.common.dialect.arp.transformer.CallTransformer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriter.Frame;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

class RewritingSignature extends Signature {
   private final String rewrite;

   RewritingSignature(String returnType, List<String> args, String rewrite) {
      super(returnType, args);
      this.rewrite = rewrite;
   }

   boolean hasRewrite() {
      return true;
   }

   public String toString() {
      return this.rewrite;
   }

   public void unparse(SqlCall originalNode, CallTransformer transformer, SqlWriter writer, int leftPrec, int rightPrec) {
      SqlOperator operator = originalNode.getOperator();
      if (leftPrec > operator.getLeftPrec() || operator.getRightPrec() <= rightPrec && rightPrec != 0 || writer.isAlwaysUseParentheses() && originalNode.isA(SqlKind.EXPRESSION)) {
         Frame frame = writer.startList("(", ")");
         this.unparse(originalNode, transformer, writer, 0, 0);
         writer.endList(frame);
      } else {
         this.doUnparse(originalNode, transformer, writer);
      }

      writer.setNeedWhitespace(true);
   }

   protected List<String> getOperatorsAsStringList(SqlCall originalNode, CallTransformer transformer, SqlWriter writer) {
      Builder<String> argsBuilder = ImmutableList.builder();
      SqlPrettyWriter tempWriter = new SqlPrettyWriter(writer.getDialect());
      tempWriter.setAlwaysUseParentheses(false);
      tempWriter.setSelectListItemsOnSeparateLines(false);
      tempWriter.setIndentation(0);
      Iterator var6 = transformer.transformSqlOperands(originalNode.getOperandList()).iterator();

      while(var6.hasNext()) {
         SqlNode operand = (SqlNode)var6.next();
         tempWriter.reset();
         operand.unparse(tempWriter, 0, 0);
         argsBuilder.add(tempWriter.toString());
      }

      return argsBuilder.build();
   }

   private void doUnparse(SqlCall originalNode, CallTransformer transformer, SqlWriter writer) {
      Object[] args = this.getOperatorsAsStringList(originalNode, transformer, writer).toArray();
      writer.print(MessageFormat.format(this.rewrite, args));
   }
}
