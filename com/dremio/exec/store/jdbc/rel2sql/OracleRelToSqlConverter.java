package com.dremio.exec.store.jdbc.rel2sql;

import com.dremio.common.rel2sql.SqlImplementor;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.Builder;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.DremioAliasContext;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.DremioContext;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.Result;
import com.dremio.common.rel2sql.SqlImplementor.Clause;
import com.dremio.common.rel2sql.SqlImplementor.Context;
import com.dremio.exec.store.jdbc.dialect.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.dialect.OracleDialect;
import com.dremio.exec.store.jdbc.rel.JdbcProject;
import com.dremio.exec.store.jdbc.rel.JdbcSort;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.Map.Entry;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

public class OracleRelToSqlConverter extends JdbcDremioRelToSqlConverter {
   private static final String ORACLE_OFFSET_ALIAS = "ORA_RNUMOFFSET$";
   private boolean canPushOrderByOut = true;

   public OracleRelToSqlConverter(JdbcDremioSqlDialect dialect) {
      super(dialect);
   }

   protected JdbcDremioRelToSqlConverter getJdbcDremioRelToSqlConverter() {
      return this;
   }

   public boolean shouldPushOrderByOut() {
      return this.canPushOrderByOut;
   }

   public Result visit(Filter e) {
      Result x = (Result)this.visitChild(0, e.getInput());
      Builder builder;
      if (this.hasWindowFunction(e, x)) {
         builder = x.builder(e, new Clause[]{Clause.SELECT}).result().builder(e, new Clause[]{Clause.WHERE});
      } else {
         builder = x.builder(e, new Clause[]{Clause.WHERE});
      }

      builder.setWhere(builder.context.toSql((RexProgram)null, e.getCondition()));
      return builder.result();
   }

   public Result visit(Sort e) {
      Result x = (Result)this.visitChild(0, e.getInput());
      x = this.visitOrderByHelper(x, e);

      try {
         this.canPushOrderByOut = false;
         Builder builder = null;
         RexBuilder rexBuilder = e.getCluster().getRexBuilder();
         if (e.fetch != null) {
            RexNode limitRex;
            if (e.offset != null) {
               builder = x.builder(e, new Clause[]{Clause.SELECT, Clause.WHERE, Clause.FETCH});
               limitRex = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, new RexNode[]{e.fetch, e.offset});
            } else {
               builder = x.builder(e, new Clause[]{Clause.WHERE, Clause.FETCH});
               limitRex = e.fetch;
            }

            RexNode rowNumLimit = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, new RexNode[]{rexBuilder.makeFlag(OracleDialect.OracleKeyWords.ROWNUM), limitRex});
            builder.setWhere(builder.context.toSql((RexProgram)null, rowNumLimit));
            if (e.offset == null) {
               x = builder.result();
            }
         }

         if (e.offset != null) {
            if (e.fetch == null) {
               builder = x.builder(e, new Clause[]{Clause.SELECT});
            }

            List<SqlNode> selectList = new ArrayList();
            RexNode offsetRowNumRex = rexBuilder.makeFlag(OracleDialect.OracleKeyWords.ROWNUM);
            SqlNode offsetRowNum = builder.context.toSql((RexProgram)null, offsetRowNumRex);
            SqlNode offsetRowNum = SqlStdOperatorTable.AS.createCall(POS, new SqlNode[]{offsetRowNum, new SqlIdentifier("ORA_RNUMOFFSET$", POS)});
            selectList.add(offsetRowNum);
            selectList.addAll(x.asSelect().getSelectList() == null ? x.qualifiedContext().fieldList() : x.asSelect().getSelectList().getList());
            Map<String, RelDataType> aliases = this.getAliases(builder);
            List<RelDataTypeField> fields = Lists.newArrayList();
            fields.add(new RelDataTypeFieldImpl("ORA_RNUMOFFSET$", 0, offsetRowNumRex.getType()));
            fields.addAll(((RelDataType)aliases.values().iterator().next()).getFieldList());
            RelRecordType recordType = new RelRecordType(fields);
            List<RexNode> rexNodes = Lists.newArrayList();
            rexNodes.add(offsetRowNumRex);
            rexNodes.addAll(rexBuilder.identityProjects(recordType).subList(1, fields.size()));
            JdbcProject project = new JdbcProject(e.getCluster(), e.getTraitSet(), e, rexNodes, recordType, ((JdbcSort)e).getPluginId());
            aliases.put((String)aliases.keySet().iterator().next(), recordType);
            if (e.fetch == null && e.getCollation().getFieldCollations().isEmpty()) {
               builder.setSelect(new SqlNodeList(selectList, SqlImplementor.POS));
            } else {
               builder.setSelect(new SqlNodeList(this.getIdentifiers(selectList, 1), SqlImplementor.POS));
            }

            List<Clause> clauses = ImmutableList.of(Clause.SELECT);
            x = this.result(builder.result().asSelect(), clauses, project, aliases);
            builder = x.builder(e, new Clause[]{Clause.SELECT, Clause.WHERE});
            builder.setSelect(new SqlNodeList(this.getIdentifiers(selectList.subList(1, selectList.size()), 0), SqlImplementor.POS));
            RexNode rowNumOffsetClause = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, new RexNode[]{rexBuilder.makeInputRef(project, 0), e.offset});
            builder.setWhere(x.qualifiedContext().toSql((RexProgram)null, rowNumOffsetClause));
            x = builder.result();
         }
      } finally {
         this.canPushOrderByOut = true;
      }

      return x;
   }

   private Collection<? extends SqlNode> getIdentifiers(List<SqlNode> nodeList, int startTransformIndex) {
      com.google.common.collect.ImmutableList.Builder<SqlNode> nodeListBuilder = new com.google.common.collect.ImmutableList.Builder();

      for(int i = 0; i < nodeList.size(); ++i) {
         SqlNode node = (SqlNode)nodeList.get(i);
         if (i >= startTransformIndex && node.getKind() == SqlKind.AS) {
            nodeListBuilder.add(((SqlCall)node).operand(1));
         } else if (i >= startTransformIndex && node.getKind() == SqlKind.IDENTIFIER) {
            SqlIdentifier identNode = (SqlIdentifier)node;
            nodeListBuilder.add(new SqlIdentifier((String)identNode.names.get(identNode.names.size() - 1), identNode.getParserPosition()));
         } else {
            nodeListBuilder.add(node);
         }
      }

      return nodeListBuilder.build();
   }

   public Context aliasContext(Map<String, RelDataType> aliases, boolean qualified) {
      return new OracleRelToSqlConverter.OracleAliasContext(aliases, qualified);
   }

   private Map<String, RelDataType> getAliases(Builder builder) {
      if (builder.context instanceof DremioAliasContext) {
         return new HashMap(((DremioAliasContext)builder.context).getAliases());
      } else {
         Map<String, RelDataType> aliases = Maps.newHashMap();
         aliases.put((String)((Entry)builder.getAliases().entrySet().iterator().next()).getKey(), builder.getNeedType());
         return aliases;
      }
   }

   public SqlWindow adjustWindowForSource(DremioContext context, SqlAggFunction op, SqlWindow window) {
      List<SqlAggFunction> opsToAddOrderByTo = ImmutableList.of(SqlStdOperatorTable.ROW_NUMBER, SqlStdOperatorTable.LAG, SqlStdOperatorTable.LEAD, SqlStdOperatorTable.NTILE);
      return addDummyOrderBy(window, context, op, opsToAddOrderByTo);
   }

   class OracleAliasContext extends DremioAliasContext {
      public OracleAliasContext(Map<String, RelDataType> aliases, boolean qualified) {
         super(OracleRelToSqlConverter.this, aliases, qualified);
      }

      public SqlCall toSql(RexProgram program, RexOver rexOver) {
         SqlCall sqlCall = super.toSql(program, rexOver);
         return rexOver.getType().getSqlTypeName().equals(SqlTypeName.DOUBLE) ? (SqlCall)this.checkAndAddFloatCast(rexOver, sqlCall) : sqlCall;
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
