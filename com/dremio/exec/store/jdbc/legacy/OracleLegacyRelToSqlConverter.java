package com.dremio.exec.store.jdbc.legacy;

import com.dremio.common.rel2sql.DremioRelToSqlConverter.Builder;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.DremioContext;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.Result;
import com.dremio.common.rel2sql.SqlImplementor.Clause;
import com.dremio.exec.store.jdbc.rel2sql.JdbcDremioRelToSqlConverter;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class OracleLegacyRelToSqlConverter extends JdbcDremioRelToSqlConverter {
   private boolean canPushOrderByOut = true;

   public OracleLegacyRelToSqlConverter(JdbcDremioSqlDialect dialect) {
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
      Builder builder = x.builder(e, new Clause[]{Clause.WHERE});
      builder.setWhere(builder.context.toSql((RexProgram)null, e.getCondition()));
      return builder.result();
   }

   public Result visit(Sort e) {
      Result x = (Result)this.visitChild(0, e.getInput());
      x = this.visitOrderByHelper(x, e);
      if (e.fetch != null) {
         try {
            this.canPushOrderByOut = false;
            Builder builder = x.builder(e, new Clause[]{Clause.WHERE, Clause.FETCH});
            RexBuilder rexBuilder = e.getCluster().getRexBuilder();
            RexNode rowNumLimit = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, new RexNode[]{rexBuilder.makeFlag(OracleLegacyDialect.OracleKeyWords.ROWNUM), e.fetch});
            builder.setWhere(builder.context.toSql((RexProgram)null, rowNumLimit));
            x = builder.result();
         } finally {
            this.canPushOrderByOut = true;
         }
      }

      return x;
   }

   public SqlWindow adjustWindowForSource(DremioContext context, SqlAggFunction op, SqlWindow window) {
      List<SqlAggFunction> opsToAddOrderByTo = ImmutableList.of(SqlStdOperatorTable.ROW_NUMBER, SqlStdOperatorTable.LAG, SqlStdOperatorTable.LEAD, SqlStdOperatorTable.NTILE);
      return addDummyOrderBy(window, context, op, opsToAddOrderByTo);
   }
}
