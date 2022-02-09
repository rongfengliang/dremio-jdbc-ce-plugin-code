package com.dremio.exec.store.jdbc.rel2sql;

import com.dremio.common.rel2sql.DremioRelToSqlConverter.Builder;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.Result;
import com.dremio.common.rel2sql.SqlImplementor.Clause;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.JdbcRelImpl;
import com.dremio.exec.store.jdbc.dialect.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.rel.JdbcFilter;
import com.dremio.exec.store.jdbc.rel.JdbcSort;
import com.dremio.exec.store.jdbc.rel.JdbcUnion;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.lang3.tuple.MutablePair;

public class MySQLRelToSqlConverter extends JdbcDremioRelToSqlConverter {
   private final ArrayDeque<MutablePair<Project, Boolean>> projectionContext = new ArrayDeque();

   public MySQLRelToSqlConverter(JdbcDremioSqlDialect dialect) {
      super(dialect);
   }

   protected JdbcDremioRelToSqlConverter getJdbcDremioRelToSqlConverter() {
      return this;
   }

   public Result visit(Project project) {
      MutablePair<Project, Boolean> pair = new MutablePair(project, Boolean.FALSE);
      this.projectionContext.addLast(pair);
      Result res = (Result)this.visitChild(0, project.getInput());
      if (!(Boolean)pair.right) {
         res = this.processProjectChild(project, res);
      }

      this.projectionContext.removeLast();
      return res;
   }

   public Result visit(Join e) {
      if (e.getJoinType() != JoinRelType.FULL) {
         return (Result)super.visit((Join)e);
      } else {
         Join leftJoin = duplicateJoinAndChangeType(e, JoinRelType.LEFT);
         Join rightJoin = duplicateJoinAndChangeType(e, JoinRelType.RIGHT);
         RexBuilder builder = e.getCluster().getRexBuilder();
         RexNode whereCondition = builder.makeCall(SqlStdOperatorTable.IS_NULL, new RexNode[]{builder.makeInputRef(e.getLeft(), 0)});
         StoragePluginId pluginId = e instanceof JdbcRelImpl ? ((JdbcRelImpl)e).getPluginId() : null;
         JdbcFilter filter = new JdbcFilter(e.getCluster(), e.getTraitSet(), rightJoin, whereCondition, ImmutableSet.of(), pluginId);
         Object leftUnionOp;
         Object rightUnionOp;
         if (this.projectionContext.isEmpty()) {
            leftUnionOp = leftJoin;
            rightUnionOp = filter;
         } else {
            MutablePair<Project, Boolean> originalProjectPair = (MutablePair)this.projectionContext.getLast();
            Project originalProject = (Project)originalProjectPair.left;
            leftUnionOp = originalProject.copy(originalProject.getTraitSet(), Collections.singletonList(leftJoin));
            rightUnionOp = originalProject.copy(originalProject.getTraitSet(), Collections.singletonList(filter));
            originalProjectPair.right = Boolean.TRUE;
         }

         List<RelNode> nodes = Lists.newArrayList(new RelNode[]{(RelNode)leftUnionOp, (RelNode)rightUnionOp});
         JdbcUnion union = new JdbcUnion(e.getCluster(), e.getTraitSet(), nodes, true, pluginId);
         return (Result)super.visit((Union)union);
      }
   }

   protected Result visitFetchAndOffsetHelper(Result x, Sort e) {
      Builder builder;
      if (e.fetch != null) {
         builder = x.builder(e, new Clause[]{Clause.FETCH});
         builder.setFetch(builder.context.toSql((RexProgram)null, e.fetch));
         x = builder.result();
      }

      if (!JdbcSort.isOffsetEmpty(e)) {
         builder = x.builder(e, new Clause[]{Clause.OFFSET});
         builder.setOffset(builder.context.toSql((RexProgram)null, e.offset));
         x = builder.result();
      }

      return x;
   }

   private static Join duplicateJoinAndChangeType(Join e, JoinRelType type) {
      return e.copy(e.getTraitSet(), e.getCondition(), e.getLeft(), e.getRight(), type, e.isSemiJoinDone());
   }
}
