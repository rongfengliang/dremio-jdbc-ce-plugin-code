package com.dremio.exec.store.jdbc.rel2sql;

import com.dremio.common.rel2sql.DremioRelToSqlConverter;
import com.dremio.common.rel2sql.SqlImplementor;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.Builder;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.DremioContext;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.Result;
import com.dremio.common.rel2sql.SqlImplementor.Clause;
import com.dremio.common.rel2sql.SqlImplementor.Context;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.store.jdbc.dialect.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.rel.JdbcProject;
import com.dremio.exec.store.jdbc.rel.JdbcTableScan;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.Map.Entry;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.rex.WindowUtil;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

public abstract class JdbcDremioRelToSqlConverter extends DremioRelToSqlConverter {
   private boolean topLevelExpandBoolean = false;
   private Map<String, Map<String, String>> columnProperties = new HashMap();

   public JdbcDremioRelToSqlConverter(JdbcDremioSqlDialect dremioDialect) {
      super(dremioDialect);
   }

   public DremioRelToSqlConverter getDremioRelToSqlConverter() {
      return this.getJdbcDremioRelToSqlConverter();
   }

   protected abstract JdbcDremioRelToSqlConverter getJdbcDremioRelToSqlConverter();

   public void setColumnProperties(Map<String, Map<String, String>> columnProperties) {
      this.columnProperties = columnProperties;
   }

   protected Map<String, Map<String, String>> getColumnProperties() {
      return this.columnProperties;
   }

   public Result visit(Filter e) {
      RelNode input = e.getInput();
      if (e.getInput() instanceof Aggregate) {
         return (Result)super.visit(e);
      } else {
         Result x = (Result)this.visitChild(0, input);
         this.parseCorrelTable(e, x);
         Builder builder;
         if (this.hasWindowFunction(e, x)) {
            builder = x.builder(e, new Clause[]{Clause.SELECT}).result().builder(e, new Clause[]{Clause.WHERE});
         } else {
            builder = x.builder(e, new Clause[]{Clause.WHERE});
         }

         RexNode condition = e.getCondition() != null ? this.simplifyDatetimePlus(e.getCondition(), e.getCluster().getRexBuilder()) : null;
         builder.setWhere(builder.context.toSql((RexProgram)null, condition));
         return builder.result();
      }
   }

   public com.dremio.common.rel2sql.SqlImplementor.Result visit(Window e) {
      this.windowStack.push(e);
      com.dremio.common.rel2sql.SqlImplementor.Result x = this.visitChild(0, e.getInput());
      this.windowStack.pop();
      com.dremio.common.rel2sql.SqlImplementor.Builder builder = x.builder(e, new Clause[0]);
      RelNode input = e.getInput();
      List<RexOver> rexOvers = WindowUtil.getOver(e);
      List<SqlNode> selectList = new ArrayList();
      Iterator var7;
      if (!(input instanceof JdbcProject) || !((JdbcProject)input).isDummyProject()) {
         var7 = input.getRowType().getFieldList().iterator();

         while(var7.hasNext()) {
            RelDataTypeField field = (RelDataTypeField)var7.next();
            this.addSelect(selectList, builder.context.field(field.getIndex()), e.getRowType());
         }
      }

      var7 = rexOvers.iterator();

      while(var7.hasNext()) {
         RexOver rexOver = (RexOver)var7.next();
         this.addSelect(selectList, builder.context.toSql((RexProgram)null, rexOver), e.getRowType());
      }

      builder.setSelect(new SqlNodeList(selectList, POS));
      return builder.result();
   }

   public Context aliasContext(Map<String, RelDataType> aliases, boolean qualified) {
      return new JdbcDremioRelToSqlConverter.JdbcDremioAliasContext(aliases, qualified);
   }

   public Context joinContext(Context leftContext, Context rightContext) {
      return new JdbcDremioRelToSqlConverter.JdbcDremioJoinContext(leftContext, rightContext);
   }

   public Context selectListContext(SqlNodeList selectList) {
      return new JdbcDremioRelToSqlConverter.JdbcDremioSelectListContext(selectList);
   }

   protected boolean hasWindowFunction(Filter filter, Result result) {
      return filter.getCondition().accept(new JdbcDremioRelToSqlConverter.HasWindowVisitor(result.builder(filter.getInput(), new Clause[]{Clause.SELECT}).context)) != null;
   }

   protected Result processProjectChild(Project project, Result childResult) {
      boolean originalExpandBoolean = this.topLevelExpandBoolean;

      Result var4;
      try {
         this.topLevelExpandBoolean = true;
         var4 = super.processProjectChild(project, childResult);
      } finally {
         this.topLevelExpandBoolean = originalExpandBoolean;
      }

      return var4;
   }

   protected SqlNode addCastIfNeeded(SqlIdentifier expr, RelDataType type) {
      if (this.shouldAddExplicitCast(expr)) {
         SqlIdentifier typeIdentifier = new SqlIdentifier(type.getSqlTypeName().name(), SqlParserPos.ZERO);
         SqlDataTypeSpec spec = new SqlDataTypeSpec(typeIdentifier, type.getPrecision(), type.getScale(), (String)null, (TimeZone)null, SqlParserPos.ZERO);
         return SqlStdOperatorTable.CAST.createCall(POS, new SqlNode[]{expr, spec});
      } else {
         return expr;
      }
   }

   protected boolean expandBooleanToBitExpr() {
      return this.getDialect().mapBooleanToBitExpr();
   }

   protected boolean shouldAddExplicitCast(SqlNode node) {
      if (node.getKind() == SqlKind.IDENTIFIER) {
         SqlIdentifier identNode = (SqlIdentifier)node;
         String lowerCaseName = ((String)Util.last(identNode.names)).toLowerCase(Locale.ROOT);
         Map<String, String> properties = (Map)this.columnProperties.get(lowerCaseName);
         if (properties != null) {
            String explicitCast = (String)properties.get("explicitCast");
            return Boolean.TRUE.toString().equals(explicitCast);
         }
      }

      return false;
   }

   private static class HasWindowVisitor extends RexVisitorImpl<Boolean> {
      private final Context context;

      HasWindowVisitor(Context context) {
         super(true);
         this.context = context;
      }

      public Boolean visitInputRef(RexInputRef inputRef) {
         return this.context.field(inputRef.getIndex()).getKind() == SqlKind.OVER ? Boolean.TRUE : null;
      }

      public Boolean visitCall(RexCall call) {
         return this.visitChildren(call.operands);
      }

      public Boolean visitSubQuery(RexSubQuery subquery) {
         return this.visitChildren(MoreRelOptUtil.getChildExps(subquery.rel));
      }

      public Boolean visitOver(RexOver over) {
         return Boolean.TRUE;
      }

      private Boolean visitChildren(List<RexNode> childExps) {
         Iterator var2 = childExps.iterator();

         RexNode operand;
         do {
            if (!var2.hasNext()) {
               return null;
            }

            operand = (RexNode)var2.next();
         } while(null == operand.accept(this));

         return Boolean.TRUE;
      }
   }

   protected class JdbcDremioSelectListContext extends JdbcDremioRelToSqlConverter.JdbcDremioContext {
      private final SqlNodeList selectList;

      protected JdbcDremioSelectListContext(SqlNodeList selectList) {
         super(selectList.size());
         this.selectList = selectList;
      }

      public SqlNode field(int ordinal) {
         SqlNode selectItem = this.selectList.get(ordinal);
         switch(selectItem.getKind()) {
         case AS:
            return ((SqlCall)selectItem).operand(0);
         default:
            return selectItem;
         }
      }
   }

   protected class JdbcDremioJoinContext extends JdbcDremioRelToSqlConverter.JdbcDremioContext {
      private final DremioContext leftContext;
      private final DremioContext rightContext;

      protected JdbcDremioJoinContext(Context leftContext, Context rightContext) {
         super(leftContext.fieldList().size() + rightContext.fieldList().size());
         Preconditions.checkArgument(leftContext instanceof DremioContext);
         Preconditions.checkArgument(rightContext instanceof DremioContext);
         this.leftContext = (DremioContext)leftContext;
         this.rightContext = (DremioContext)rightContext;
      }

      public SqlNode field(int ordinal) {
         return ordinal < this.leftContext.fieldList().size() ? this.leftContext.field(ordinal) : this.rightContext.field(ordinal - this.leftContext.fieldList().size());
      }

      public SqlNode toSql(RexProgram program, RexNode rex) {
         return rex.getKind() == SqlKind.LITERAL && JdbcDremioRelToSqlConverter.this.expandBooleanToBitExpr() && rex.getType().getSqlTypeName() == SqlTypeName.BOOLEAN && ((RexLiteral)rex).getTypeName() != SqlTypeName.NULL ? this.checkAndExpandBoolean(program, rex) : super.toSql(program, rex);
      }
   }

   protected class JdbcDremioAliasContext extends JdbcDremioRelToSqlConverter.JdbcDremioContext {
      private final boolean qualified;
      private final Map<String, RelDataType> aliases;

      public JdbcDremioAliasContext(Map<String, RelDataType> aliases, boolean qualified) {
         super(JdbcDremioRelToSqlConverter.computeFieldCount(aliases));
         this.aliases = aliases;
         this.qualified = qualified;
      }

      public SqlNode field(int ordinal) {
         List fields;
         for(Iterator var2 = this.getAliases().entrySet().iterator(); var2.hasNext(); ordinal -= fields.size()) {
            Entry<String, RelDataType> alias = (Entry)var2.next();
            if (ordinal < 0) {
               break;
            }

            fields = ((RelDataType)alias.getValue()).getFieldList();
            if (ordinal < fields.size()) {
               RelDataTypeField field = (RelDataTypeField)fields.get(ordinal);
               SqlNode mappedSqlNode = (SqlNode)JdbcDremioRelToSqlConverter.this.ordinalMap.get(field.getName().toLowerCase(Locale.ROOT));
               if (mappedSqlNode != null) {
                  return mappedSqlNode;
               }

               SqlCollation collation = field.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER && JdbcDremioRelToSqlConverter.this.canAddCollation(field) ? JdbcDremioRelToSqlConverter.this.getDialect().getDefaultCollation(SqlKind.IDENTIFIER) : null;
               return new SqlIdentifier(!this.qualified && !JdbcDremioRelToSqlConverter.this.getJdbcDremioRelToSqlConverter().isSubQuery() ? ImmutableList.of(field.getName()) : ImmutableList.of((String)alias.getKey(), field.getName()), collation, SqlImplementor.POS, (List)null);
            }
         }

         throw new AssertionError("field ordinal " + ordinal + " out of range " + this.getAliases());
      }

      public Map<String, RelDataType> getAliases() {
         return this.aliases;
      }
   }

   public abstract class JdbcDremioContext extends DremioContext {
      protected JdbcDremioContext(int fieldCount) {
         super(JdbcDremioRelToSqlConverter.this, fieldCount);
      }

      protected SqlNode checkAndExpandBoolean(RexProgram program, RexNode node) {
         if (node.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
            switch(node.getKind()) {
            case INPUT_REF:
               return SqlStdOperatorTable.EQUALS.createCall(new SqlNodeList(ImmutableList.of(this.toSql(program, node), SqlNumericLiteral.createExactNumeric("1", SqlImplementor.POS)), SqlImplementor.POS));
            case LITERAL:
               RexLiteral literal = (RexLiteral)node;
               if (literal.getTypeName() == SqlTypeName.BOOLEAN) {
                  return this.wrapInEqualsOne(SqlNumericLiteral.createExactNumeric(node.isAlwaysTrue() ? "1" : "0", SqlImplementor.POS));
               }
               break;
            case NOT:
               RexCall call = (RexCall)node;
               RexNode child = (RexNode)call.getOperands().get(0);
               if (child.getKind() != SqlKind.INPUT_REF && child.getKind() != SqlKind.LITERAL) {
                  SqlNode sqlChild = this.toSql(program, child);
                  if (sqlChild.getKind() == SqlKind.CASE) {
                     return SqlStdOperatorTable.NOT.createCall(SqlNodeList.of(this.wrapInEqualsOne(sqlChild)));
                  }

                  return SqlStdOperatorTable.NOT.createCall(SqlNodeList.of(sqlChild));
               }

               return SqlStdOperatorTable.NOT.createCall(SqlNodeList.of(this.wrapInEqualsOne(this.toSql(program, child))));
            }
         }

         return this.toSql(program, node);
      }

      public SqlNode toSql(RexProgram program, RexNode rex) {
         RexCall call;
         ArrayList whenNodes;
         switch(rex.getKind()) {
         case LITERAL:
            if (JdbcDremioRelToSqlConverter.this.expandBooleanToBitExpr() && rex.getType().getSqlTypeName() == SqlTypeName.BOOLEAN && ((RexLiteral)rex).getTypeName() != SqlTypeName.NULL) {
               return SqlNumericLiteral.createExactNumeric(rex.isAlwaysTrue() ? "1" : "0", SqlImplementor.POS);
            }
            break;
         case NOT:
            if (JdbcDremioRelToSqlConverter.this.topLevelExpandBoolean && JdbcDremioRelToSqlConverter.this.expandBooleanToBitExpr() && rex.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
               return this.wrapInCase(this.checkAndExpandBoolean(program, rex));
            }
            break;
         case AND:
         case OR:
            if (JdbcDremioRelToSqlConverter.this.expandBooleanToBitExpr()) {
               call = (RexCall)rex;
               whenNodes = new ArrayList();
               boolean originalExpandBoolean = JdbcDremioRelToSqlConverter.this.topLevelExpandBoolean;

               SqlCall var24;
               try {
                  JdbcDremioRelToSqlConverter.this.topLevelExpandBoolean = false;
                  Iterator var22 = call.getOperands().iterator();

                  while(var22.hasNext()) {
                     RexNode node = (RexNode)var22.next();
                     whenNodes.add(this.checkAndExpandBoolean(program, node));
                     if (2 == whenNodes.size()) {
                        whenNodes.set(0, call.getOperator().createCall(new SqlNodeList(whenNodes, SqlImplementor.POS)));
                        whenNodes.remove(1);
                     }
                  }

                  if (!originalExpandBoolean) {
                     SqlNode var25 = (SqlNode)whenNodes.get(0);
                     return var25;
                  }

                  var24 = this.wrapInCase((SqlNode)whenNodes.get(0));
               } finally {
                  JdbcDremioRelToSqlConverter.this.topLevelExpandBoolean = originalExpandBoolean;
               }

               return var24;
            }
            break;
         case CASE:
            if (JdbcDremioRelToSqlConverter.this.expandBooleanToBitExpr()) {
               call = (RexCall)rex;
               if (call.getOperands().size() % 2 == 0) {
                  return super.toSql(program, rex);
               }

               whenNodes = new ArrayList();
               List<SqlNode> thenNodes = new ArrayList();
               boolean originalExpandBooleanx = JdbcDremioRelToSqlConverter.this.topLevelExpandBoolean;
               boolean thenIsBoolean = false;

               for(int i = 0; i < call.getOperands().size() - 1; i += 2) {
                  try {
                     JdbcDremioRelToSqlConverter.this.topLevelExpandBoolean = false;
                     whenNodes.add(this.checkAndExpandBoolean(program, (RexNode)call.getOperands().get(i)));
                  } finally {
                     JdbcDremioRelToSqlConverter.this.topLevelExpandBoolean = originalExpandBooleanx;
                  }

                  RexNode childRex = (RexNode)call.getOperands().get(i + 1);
                  thenIsBoolean = childRex.getType().getSqlTypeName() == SqlTypeName.BOOLEAN;
                  thenNodes.add(this.checkAndWrapThenInCase(program, childRex));
               }

               SqlNode elseNode = this.checkAndWrapThenInCase(program, (RexNode)call.getOperands().get(call.getOperands().size() - 1));
               SqlNode sqlCase = new SqlCase(SqlImplementor.POS, (SqlNode)null, new SqlNodeList(whenNodes, SqlImplementor.POS), new SqlNodeList(thenNodes, SqlImplementor.POS), elseNode);
               if (thenIsBoolean && !JdbcDremioRelToSqlConverter.this.topLevelExpandBoolean) {
                  return this.wrapInEqualsOne(sqlCase);
               }

               return sqlCase;
            }
            break;
         case SCALAR_QUERY:
         case EXISTS:
            RexSubQuery subQuery = (RexSubQuery)rex;
            SqlOperator subQueryOperator = subQuery.getOperator();
            JdbcDremioRelToSqlConverter.this.outerQueryAliasContextStack.push(this);
            Result subQueryResult;
            if (subQuery.rel instanceof JdbcTableScan && rex.getKind() == SqlKind.EXISTS) {
               JdbcTableScan tableScan = (JdbcTableScan)subQuery.rel;
               RexLiteral literalOne = tableScan.getCluster().getRexBuilder().makeBigintLiteral(BigDecimal.ONE);
               FieldInfoBuilder builder = tableScan.getCluster().getTypeFactory().builder();
               builder.add("EXPR", literalOne.getType());
               JdbcProject project = new JdbcProject(tableScan.getCluster(), tableScan.getTraitSet(), tableScan, ImmutableList.of(literalOne), builder.build(), tableScan.getPluginId());
               subQueryResult = (Result)JdbcDremioRelToSqlConverter.this.visitChild(0, project);
            } else {
               subQueryResult = (Result)JdbcDremioRelToSqlConverter.this.visitChild(0, subQuery.rel);
            }

            JdbcDremioRelToSqlConverter.this.outerQueryAliasContextStack.pop();
            List<SqlNode> operands = this.toSql(program, subQuery.getOperands());
            operands.add(subQueryResult.asNode());
            return subQueryOperator.createCall(new SqlNodeList(operands, org.apache.calcite.rel.rel2sql.SqlImplementor.POS));
         case EQUALS:
            if (JdbcDremioRelToSqlConverter.this.expandBooleanToBitExpr()) {
               call = (RexCall)rex;
               if (((RexNode)call.getOperands().get(0)).isAlwaysTrue()) {
                  return this.checkAndExpandBoolean(program, (RexNode)call.getOperands().get(1));
               }

               if (((RexNode)call.getOperands().get(1)).isAlwaysTrue()) {
                  return this.checkAndExpandBoolean(program, (RexNode)call.getOperands().get(0));
               }

               if (((RexNode)call.getOperands().get(0)).isAlwaysFalse()) {
                  return SqlStdOperatorTable.NOT.createCall(SqlNodeList.of(this.checkAndExpandBoolean(program, (RexNode)call.getOperands().get(1))));
               }

               if (((RexNode)call.getOperands().get(1)).isAlwaysFalse()) {
                  return SqlStdOperatorTable.NOT.createCall(SqlNodeList.of(this.checkAndExpandBoolean(program, (RexNode)call.getOperands().get(0))));
               }
            }
         case LIKE:
         case IS_NULL:
         case IS_NOT_NULL:
         case NOT_EQUALS:
         case GREATER_THAN:
         case GREATER_THAN_OR_EQUAL:
         case LESS_THAN:
         case LESS_THAN_OR_EQUAL:
            if (JdbcDremioRelToSqlConverter.this.topLevelExpandBoolean && JdbcDremioRelToSqlConverter.this.expandBooleanToBitExpr() && rex.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
               return this.wrapInCase(super.toSql(program, rex));
            }
         }

         return super.toSql(program, rex);
      }

      private SqlCall wrapInEqualsOne(SqlNode node) {
         return SqlStdOperatorTable.EQUALS.createCall(SqlNodeList.of(node, SqlNumericLiteral.createExactNumeric("1", SqlImplementor.POS)));
      }

      private SqlNode checkAndWrapThenInCase(RexProgram program, RexNode rex) {
         switch(rex.getKind()) {
         case INPUT_REF:
         case LITERAL:
         case CAST:
            return this.toSql(program, rex);
         default:
            SqlNode childSql = this.checkAndExpandBoolean(program, rex);
            return (SqlNode)(rex.getType().getSqlTypeName() == SqlTypeName.BOOLEAN && !childSql.getKind().equals(SqlKind.CASE) ? this.wrapInCase(childSql) : childSql);
         }
      }

      private SqlCall wrapInCase(SqlNode node) {
         List<SqlNode> possibleNullNodes = new ArrayList();
         this.getIsNullChildren(node, possibleNullNodes);
         if (possibleNullNodes.isEmpty()) {
            return SqlStdOperatorTable.CASE.createCall(SqlImplementor.POS, new SqlNode[]{null, SqlNodeList.of(node), SqlNodeList.of(SqlNumericLiteral.createExactNumeric("1", SqlImplementor.POS)), SqlNumericLiteral.createExactNumeric("0", SqlImplementor.POS)});
         } else {
            for(int i = 1; i < possibleNullNodes.size(); ++i) {
               possibleNullNodes.set(0, SqlStdOperatorTable.OR.createCall(SqlNodeList.of((SqlNode)possibleNullNodes.get(0), (SqlNode)possibleNullNodes.get(i))));
            }

            return SqlStdOperatorTable.CASE.createCall(SqlImplementor.POS, new SqlNode[]{null, SqlNodeList.of((SqlNode)possibleNullNodes.get(0), node), SqlNodeList.of(SqlLiteral.createNull(SqlImplementor.POS), SqlNumericLiteral.createExactNumeric("1", SqlImplementor.POS)), SqlNumericLiteral.createExactNumeric("0", SqlImplementor.POS)});
         }
      }

      private void getIsNullChildren(SqlNode node, List<SqlNode> children) {
         switch(node.getKind()) {
         case LITERAL:
            SqlLiteral literal = (SqlLiteral)node;
            if (literal.getTypeName() != SqlTypeName.NULL) {
               break;
            }
         case CASE:
         case SCALAR_QUERY:
         case EXISTS:
         default:
            children.add(SqlStdOperatorTable.IS_NULL.createCall(SqlNodeList.of(node)));
            break;
         case NOT:
         case AND:
         case OR:
         case EQUALS:
         case LIKE:
         case NOT_EQUALS:
         case GREATER_THAN:
         case GREATER_THAN_OR_EQUAL:
         case LESS_THAN:
         case LESS_THAN_OR_EQUAL:
            if (node instanceof SqlCall) {
               Iterator var3 = ((SqlCall)node).getOperandList().iterator();

               while(var3.hasNext()) {
                  SqlNode child = (SqlNode)var3.next();
                  this.getIsNullChildren(child, children);
               }
            }
         case IS_NULL:
         case IS_NOT_NULL:
         }

      }
   }
}
