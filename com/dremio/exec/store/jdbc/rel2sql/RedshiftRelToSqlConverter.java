package com.dremio.exec.store.jdbc.rel2sql;

import com.dremio.common.rel2sql.DremioRelToSqlConverter.Builder;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.DremioContext;
import com.dremio.exec.store.jdbc.legacy.JdbcDremioSqlDialect;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

public class RedshiftRelToSqlConverter extends JdbcDremioRelToSqlConverter {
   public RedshiftRelToSqlConverter(JdbcDremioSqlDialect dialect) {
      super(dialect);
   }

   protected JdbcDremioRelToSqlConverter getJdbcDremioRelToSqlConverter() {
      return this;
   }

   public void addSelect(List<SqlNode> selectList, SqlNode node, RelDataType rowType) {
      String name = (String)rowType.getFieldNames().get(selectList.size());
      String alias = SqlValidatorUtil.getAlias((SqlNode)node, -1);
      if (alias == null || !alias.equals(name)) {
         if (name.equals("*")) {
            node = SqlStdOperatorTable.AS.createCall(POS, new SqlNode[]{(SqlNode)node, new SqlIdentifier(SqlUtil.deriveAliasFromOrdinal(selectList.size()), POS)});
         } else {
            node = SqlStdOperatorTable.AS.createCall(POS, new SqlNode[]{(SqlNode)node, new SqlIdentifier(name, POS)});
         }
      }

      selectList.add(node);
   }

   protected void generateGroupBy(Builder builder, Aggregate e) {
      List<SqlNode> groupByList = Expressions.list();
      List<SqlNode> selectList = new ArrayList();
      Iterator var5 = e.getGroupSet().iterator();

      while(true) {
         while(true) {
            SqlNode field;
            do {
               int group;
               if (!var5.hasNext()) {
                  for(var5 = e.getAggCallList().iterator(); var5.hasNext(); this.addSelect(selectList, field, e.getRowType())) {
                     AggregateCall aggCall = (AggregateCall)var5.next();
                     field = ((DremioContext)builder.context).toSql(aggCall, e);
                     if (aggCall.getAggregation() instanceof SqlSingleValueAggFunction) {
                        field = this.dialect.rewriteSingleValueExpr(field);
                     }
                  }

                  builder.setSelect(new SqlNodeList(selectList, POS));
                  if (groupByList.isEmpty()) {
                     if (e.getAggCallList().isEmpty()) {
                        var5 = e.getGroupSet().iterator();

                        while(var5.hasNext()) {
                           group = (Integer)var5.next();
                           groupByList.add(SqlLiteral.createExactNumeric(String.valueOf(group + 1), SqlParserPos.ZERO));
                        }

                        builder.setGroupBy(new SqlNodeList(groupByList, POS));
                     }
                  } else {
                     builder.setGroupBy(new SqlNodeList(groupByList, POS));
                  }

                  return;
               }

               group = (Integer)var5.next();
               field = builder.context.field(group);
               this.addSelect(selectList, field, e.getRowType());
            } while(field.getKind() == SqlKind.LITERAL && ((SqlLiteral)field).getTypeName() == SqlTypeName.NULL);

            if (field.getKind() == SqlKind.IDENTIFIER && ((SqlIdentifier)field).getCollation() != null) {
               groupByList.add(new SqlIdentifier(((SqlIdentifier)field).names, (SqlCollation)null, field.getParserPosition(), (List)null));
            } else {
               groupByList.add(field);
            }
         }
      }
   }
}
