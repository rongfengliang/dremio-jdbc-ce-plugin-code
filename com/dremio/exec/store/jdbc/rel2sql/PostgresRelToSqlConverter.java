package com.dremio.exec.store.jdbc.rel2sql;

import com.dremio.common.rel2sql.SqlImplementor;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.Builder;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.DremioAliasContext;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.DremioContext;
import com.dremio.common.rel2sql.SqlImplementor.Context;
import com.dremio.exec.store.jdbc.legacy.JdbcDremioSqlDialect;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

public class PostgresRelToSqlConverter extends JdbcDremioRelToSqlConverter {
   public PostgresRelToSqlConverter(JdbcDremioSqlDialect dialect) {
      super(dialect);
   }

   protected JdbcDremioRelToSqlConverter getJdbcDremioRelToSqlConverter() {
      return this;
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
                  Object aggCallSqlNode;
                  for(var5 = e.getAggCallList().iterator(); var5.hasNext(); this.addSelect(selectList, (SqlNode)aggCallSqlNode, e.getRowType())) {
                     AggregateCall aggCall = (AggregateCall)var5.next();
                     aggCallSqlNode = ((DremioContext)builder.context).toSql(aggCall, e);
                     if (aggCall.getAggregation() instanceof SqlSingleValueAggFunction) {
                        aggCallSqlNode = this.dialect.rewriteSingleValueExpr((SqlNode)aggCallSqlNode);
                     }

                     if (isDecimal(aggCall.getType())) {
                        aggCallSqlNode = SqlStdOperatorTable.CAST.createCall(POS, new SqlNode[]{(SqlNode)aggCallSqlNode, this.getDialect().getCastSpec(aggCall.getType())});
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

                        if (groupByList.isEmpty()) {
                           builder.setGroupBy((SqlNodeList)null);
                        } else {
                           builder.setGroupBy(new SqlNodeList(groupByList, POS));
                        }
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

   protected boolean canAddCollation(RelDataTypeField field) {
      if (field.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER) {
         String lowerCaseName = field.getName().toLowerCase(Locale.ROOT);
         Map<String, String> properties = (Map)this.columnProperties.get(lowerCaseName);
         if (properties != null) {
            String typeName = (String)properties.get("sourceTypeName");
            if (typeName != null) {
               byte var6 = -1;
               switch(typeName.hashCode()) {
               case -1382823772:
                  if (typeName.equals("bpchar")) {
                     var6 = 0;
                  }
                  break;
               case 3052374:
                  if (typeName.equals("char")) {
                     var6 = 1;
                  }
                  break;
               case 3556653:
                  if (typeName.equals("text")) {
                     var6 = 2;
                  }
                  break;
               case 236613373:
                  if (typeName.equals("varchar")) {
                     var6 = 3;
                  }
               }

               switch(var6) {
               case 0:
               case 1:
               case 2:
               case 3:
                  return true;
               default:
                  return false;
               }
            }
         }
      }

      return super.canAddCollation(field);
   }

   public Context aliasContext(Map<String, RelDataType> aliases, boolean qualified) {
      return new PostgresRelToSqlConverter.PostgreSQLAliasContext(aliases, qualified);
   }

   class PostgreSQLAliasContext extends DremioAliasContext {
      public PostgreSQLAliasContext(Map<String, RelDataType> aliases, boolean qualified) {
         super(PostgresRelToSqlConverter.this, aliases, qualified);
      }

      public SqlNode toSql(RexProgram program, RexNode rex) {
         SqlNode sqlNode = super.toSql(program, rex);
         return rex.getKind().equals(SqlKind.LITERAL) && rex.getType().getSqlTypeName().equals(SqlTypeName.DOUBLE) ? this.checkAndAddFloatCast(rex, sqlNode) : sqlNode;
      }

      private SqlNode checkAndAddFloatCast(RexNode rex, SqlNode sqlCall) {
         SqlIdentifier typeIdentifier = new SqlIdentifier(SqlTypeName.FLOAT.name(), SqlParserPos.ZERO);
         SqlDataTypeSpec spec = new SqlDataTypeSpec(typeIdentifier, -1, -1, (String)null, (TimeZone)null, SqlParserPos.ZERO);
         return SqlStdOperatorTable.CAST.createCall(SqlImplementor.POS, new SqlNode[]{sqlCall, spec});
      }
   }
}
