package com.dremio.exec.store.jdbc.rel2sql;

import com.dremio.common.rel2sql.DremioRelToSqlConverter.Builder;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.DremioContext;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.Result;
import com.dremio.common.rel2sql.SqlImplementor.Clause;
import com.dremio.exec.store.jdbc.legacy.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.rel.JdbcSort;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

public class MSSQLRelToSqlConverter extends JdbcDremioRelToSqlConverter {
   private static final SqlFunction CONVERT_FUNCTION;
   private static final SqlNode MSSQL_ODBC_FORMAT_SPEC;

   public MSSQLRelToSqlConverter(JdbcDremioSqlDialect dialect) {
      super(dialect);
   }

   protected JdbcDremioRelToSqlConverter getJdbcDremioRelToSqlConverter() {
      return this;
   }

   public void addSelect(List<SqlNode> selectList, SqlNode node, RelDataType rowType) {
      if (node instanceof SqlIdentifier && ((SqlIdentifier)node).getCollation() != null) {
         String name = (String)rowType.getFieldNames().get(selectList.size());
         selectList.add(SqlStdOperatorTable.AS.createCall(POS, new SqlNode[]{node, new SqlIdentifier(name, POS)}));
      } else {
         super.addSelect(selectList, node, rowType);
      }

   }

   public SqlWindow adjustWindowForSource(DremioContext context, SqlAggFunction op, SqlWindow window) {
      List<SqlAggFunction> opsToAddOrderByTo = ImmutableList.of(SqlStdOperatorTable.ROW_NUMBER, SqlStdOperatorTable.CUME_DIST, SqlStdOperatorTable.LAG, SqlStdOperatorTable.LEAD, SqlStdOperatorTable.NTILE, SqlStdOperatorTable.LAST_VALUE, SqlStdOperatorTable.FIRST_VALUE);
      return addDummyOrderBy(window, context, op, opsToAddOrderByTo);
   }

   protected boolean canAddCollation(RelDataTypeField field) {
      if (field.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER) {
         String lowerCaseName = field.getName().toLowerCase(Locale.ROOT);
         Map<String, String> properties = (Map)this.columnProperties.get(lowerCaseName);
         if (properties != null) {
            String typeName = (String)properties.get("sourceTypeName");
            if (typeName != null) {
               byte var6 = -1;
               switch(typeName.hashCode()) {
               case -1327778097:
                  if (typeName.equals("nvarchar")) {
                     var6 = 3;
                  }
                  break;
               case 3052374:
                  if (typeName.equals("char")) {
                     var6 = 0;
                  }
                  break;
               case 3556653:
                  if (typeName.equals("text")) {
                     var6 = 4;
                  }
                  break;
               case 104639684:
                  if (typeName.equals("nchar")) {
                     var6 = 1;
                  }
                  break;
               case 105143963:
                  if (typeName.equals("ntext")) {
                     var6 = 2;
                  }
                  break;
               case 236613373:
                  if (typeName.equals("varchar")) {
                     var6 = 5;
                  }
               }

               switch(var6) {
               case 0:
               case 1:
               case 2:
               case 3:
               case 4:
               case 5:
                  return true;
               default:
                  return false;
               }
            }
         }
      }

      return super.canAddCollation(field);
   }

   public Result visit(Sort e) {
      Result x = (Result)this.visitChild(0, e.getInput());
      Result orderByResult = this.visitOrderByHelper(x, e);
      SqlNodeList orderByList = orderByResult.asSelect().getOrderList();
      boolean hasOffset = !JdbcSort.isOffsetEmpty(e);
      boolean hasFetch = e.fetch != null;
      boolean hadOrderByListOfLiterals = orderByList == null || orderByList.getList().isEmpty();
      if (hadOrderByListOfLiterals && !hasOffset && !hasFetch) {
         return x;
      } else {
         Builder builder;
         if (hasOffset && hasFetch && (Long)((RexLiteral)e.fetch).getValue2() == 0L) {
            builder = x.builder(e, true, new Clause[]{Clause.SELECT, Clause.FETCH, Clause.ORDER_BY});
            builder.setFetch(builder.context.toSql((RexProgram)null, e.fetch));
            List<SqlNode> newOrderByList = Expressions.list();
            Iterator var10 = e.getCollation().getFieldCollations().iterator();

            while(var10.hasNext()) {
               RelFieldCollation field = (RelFieldCollation)var10.next();
               builder.addOrderItem(newOrderByList, field);
            }

            if (!newOrderByList.isEmpty()) {
               builder.setOrderBy(new SqlNodeList(newOrderByList, POS));
            }

            return builder.result();
         } else if ((hadOrderByListOfLiterals || JdbcSort.isCollationEmpty(e)) && !hasOffset) {
            Preconditions.checkState(hasFetch);
            builder = x.builder(e, true, new Clause[]{Clause.SELECT, Clause.FETCH});
            builder.setFetch(builder.context.toSql((RexProgram)null, e.fetch));
            return builder.result();
         } else {
            x = this.visitFetchAndOffsetHelper(orderByResult, e);
            return x;
         }
      }
   }

   protected void generateGroupBy(Builder builder, Aggregate e) {
      List<SqlNode> groupByList = Expressions.list();
      List<SqlNode> selectList = new ArrayList();
      Iterator var5 = e.getGroupSet().iterator();

      while(true) {
         SqlNode aggCallSqlNode;
         do {
            if (!var5.hasNext()) {
               for(var5 = e.getAggCallList().iterator(); var5.hasNext(); this.addSelect(selectList, aggCallSqlNode, e.getRowType())) {
                  AggregateCall aggCall = (AggregateCall)var5.next();
                  aggCallSqlNode = ((DremioContext)builder.context).toSql(aggCall, e);
                  if (aggCall.getAggregation() instanceof SqlSingleValueAggFunction) {
                     aggCallSqlNode = this.dialect.rewriteSingleValueExpr(aggCallSqlNode);
                  }
               }

               builder.setSelect(new SqlNodeList(selectList, POS));
               if (!groupByList.isEmpty() || e.getAggCallList().isEmpty()) {
                  builder.setGroupBy(new SqlNodeList(groupByList, POS));
               }

               return;
            }

            int group = (Integer)var5.next();
            aggCallSqlNode = builder.context.field(group);
            this.addSelect(selectList, aggCallSqlNode, e.getRowType());
         } while(aggCallSqlNode.getKind() == SqlKind.LITERAL && ((SqlLiteral)aggCallSqlNode).getTypeName() == SqlTypeName.NULL);

         groupByList.add(aggCallSqlNode);
      }
   }

   public SqlNode convertCallToSql(DremioContext context, RexProgram program, RexCall call, boolean ignoreCast) {
      if (!ignoreCast && call.getKind() == SqlKind.CAST) {
         List<RexNode> operands = call.getOperands();
         SqlTypeName sourceType = ((RexNode)operands.get(0)).getType().getSqlTypeName();
         SqlTypeName targetType = call.getType().getSqlTypeName();
         if (SqlTypeName.DATETIME_TYPES.contains(sourceType) && SqlTypeName.CHAR_TYPES.contains(targetType)) {
            SqlNode sourceTypeNode = context.toSql(program, (RexNode)operands.get(0));
            SqlNode targetTypeNode = this.dialect.getCastSpec(call.getType());
            SqlNodeList nodeList = new SqlNodeList(ImmutableList.of(targetTypeNode, sourceTypeNode, MSSQL_ODBC_FORMAT_SPEC), SqlParserPos.ZERO);
            return CONVERT_FUNCTION.createCall(nodeList);
         } else {
            return null;
         }
      } else {
         return null;
      }
   }

   static {
      CONVERT_FUNCTION = new SqlFunction("CONVERT", SqlKind.OTHER_FUNCTION, (SqlReturnTypeInference)null, InferTypes.FIRST_KNOWN, (SqlOperandTypeChecker)null, SqlFunctionCategory.SYSTEM);
      MSSQL_ODBC_FORMAT_SPEC = SqlLiteral.createExactNumeric("121", SqlParserPos.ZERO);
   }
}
