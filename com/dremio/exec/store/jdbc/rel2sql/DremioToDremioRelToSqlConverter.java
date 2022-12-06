package com.dremio.exec.store.jdbc.rel2sql;

import com.dremio.exec.store.jdbc.dialect.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.rel2sql.utilities.AliasSanitizer;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

public class DremioToDremioRelToSqlConverter extends JdbcDremioRelToSqlConverter {
   public DremioToDremioRelToSqlConverter(JdbcDremioSqlDialect dremioDialect) {
      super(dremioDialect);
   }

   protected JdbcDremioRelToSqlConverter getJdbcDremioRelToSqlConverter() {
      return this;
   }

   public void addSelect(List<SqlNode> selectList, SqlNode node, RelDataType rowType) {
      String name = (String)rowType.getFieldNames().get(selectList.size());
      String alias = SqlValidatorUtil.getAlias((SqlNode)node, -1);
      if (alias == null || !alias.equals(name)) {
         name = AliasSanitizer.sanitizeAlias(name);
         node = SqlStdOperatorTable.AS.createCall(POS, new SqlNode[]{(SqlNode)node, new SqlIdentifier(name, POS)});
      }

      selectList.add(node);
   }
}
