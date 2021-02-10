package com.dremio.exec.store.jdbc.legacy;

import com.dremio.common.rel2sql.DremioRelToSqlConverter.Builder;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.DremioContext;
import com.dremio.common.rel2sql.DremioRelToSqlConverter.Result;
import com.dremio.common.rel2sql.SqlImplementor.Clause;
import com.dremio.exec.store.jdbc.rel.JdbcSort;
import com.dremio.exec.store.jdbc.rel2sql.JdbcDremioRelToSqlConverter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

public class MSSQLLegacyRelToSqlConverter extends JdbcDremioRelToSqlConverter {
   private static final SqlFunction CONVERT_FUNCTION;
   private static final SqlNode MSSQL_ODBC_FORMAT_SPEC;

   public MSSQLLegacyRelToSqlConverter(JdbcDremioSqlDialect dialect) {
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

   public Result visit(Sort e) {
      Result x = (Result)this.visitChild(0, e.getInput());
      Result orderByResult = this.visitOrderByHelper(x, e);
      SqlNodeList orderByList = orderByResult.asSelect().getOrderList();
      boolean hadOrderByListOfLiterals = orderByList == null || orderByList.getList().isEmpty();
      if (hadOrderByListOfLiterals && JdbcSort.isOffsetEmpty(e) && e.fetch == null) {
         return x;
      } else if ((hadOrderByListOfLiterals || JdbcSort.isCollationEmpty(e)) && JdbcSort.isOffsetEmpty(e)) {
         Preconditions.checkState(e.fetch != null);
         Builder builder = x.builder(e, true, new Clause[]{Clause.SELECT, Clause.FETCH});
         builder.setFetch(builder.context.toSql((RexProgram)null, e.fetch));
         return builder.result();
      } else {
         x = this.visitFetchAndOffsetHelper(orderByResult, e);
         return x;
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
