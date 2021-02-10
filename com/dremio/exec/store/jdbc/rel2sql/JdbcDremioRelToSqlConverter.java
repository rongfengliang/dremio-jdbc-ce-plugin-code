package com.dremio.exec.store.jdbc.rel2sql;

import com.dremio.common.rel2sql.DremioRelToSqlConverter;
import com.dremio.common.rel2sql.SqlImplementor;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.DremioContext;
import com.dremio.common.rel2sql.SqlImplementor.Builder;
import com.dremio.common.rel2sql.SqlImplementor.Clause;
import com.dremio.common.rel2sql.SqlImplementor.Context;
import com.dremio.common.rel2sql.SqlImplementor.Result;
import com.dremio.exec.store.jdbc.legacy.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.rel.JdbcProject;
import com.dremio.exec.store.jdbc.rel.JdbcTableScan;
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
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.WindowUtil;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.util.Util;

public abstract class JdbcDremioRelToSqlConverter extends DremioRelToSqlConverter {
   protected Map<String, Map<String, String>> columnProperties = new HashMap();

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

   public Result visit(Window e) {
      this.windowStack.push(e);
      Result x = this.visitChild(0, e.getInput());
      this.windowStack.pop();
      Builder builder = x.builder(e, new Clause[0]);
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

   public Context selectListContext(SqlNodeList selectList) {
      return new JdbcDremioRelToSqlConverter.JdbcDremioSelectListContext(selectList);
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

   private boolean shouldAddExplicitCast(SqlIdentifier node) {
      String lowerCaseName = ((String)Util.last(node.names)).toLowerCase(Locale.ROOT);
      Map<String, String> properties = (Map)this.columnProperties.get(lowerCaseName);
      if (properties != null) {
         String explicitCast = (String)properties.get("explicitCast");
         return Boolean.TRUE.toString().equals(explicitCast);
      } else {
         return false;
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

      public SqlNode toSql(RexProgram program, RexNode rex) {
         switch(rex.getKind()) {
         case SCALAR_QUERY:
         case EXISTS:
            RexSubQuery subQuery = (RexSubQuery)rex;
            SqlOperator subQueryOperator = subQuery.getOperator();
            JdbcDremioRelToSqlConverter.this.outerQueryAliasContextStack.push(this);
            com.dremio.common.rel2sql.DremioRelToSqlConverter.Result subQueryResult;
            if (subQuery.rel instanceof JdbcTableScan && rex.getKind() == SqlKind.EXISTS) {
               JdbcTableScan tableScan = (JdbcTableScan)subQuery.rel;
               RexLiteral literalOne = tableScan.getCluster().getRexBuilder().makeBigintLiteral(BigDecimal.ONE);
               FieldInfoBuilder builder = tableScan.getCluster().getTypeFactory().builder();
               builder.add("EXPR", literalOne.getType());
               JdbcProject project = new JdbcProject(tableScan.getCluster(), tableScan.getTraitSet(), tableScan, ImmutableList.of(literalOne), builder.build(), tableScan.getPluginId());
               subQueryResult = (com.dremio.common.rel2sql.DremioRelToSqlConverter.Result)JdbcDremioRelToSqlConverter.this.visitChild(0, project);
            } else {
               subQueryResult = (com.dremio.common.rel2sql.DremioRelToSqlConverter.Result)JdbcDremioRelToSqlConverter.this.visitChild(0, subQuery.rel);
            }

            JdbcDremioRelToSqlConverter.this.outerQueryAliasContextStack.pop();
            List<SqlNode> operands = this.toSql(program, subQuery.getOperands());
            operands.add(subQueryResult.asNode());
            return subQueryOperator.createCall(new SqlNodeList(operands, org.apache.calcite.rel.rel2sql.SqlImplementor.POS));
         default:
            return super.toSql(program, rex);
         }
      }
   }
}
