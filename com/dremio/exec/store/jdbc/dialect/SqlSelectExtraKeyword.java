package com.dremio.exec.store.jdbc.dialect;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;

public enum SqlSelectExtraKeyword {
   TOP;

   public SqlLiteral symbol(SqlParserPos pos) {
      return SqlLiteral.createSymbol(this, pos);
   }
}
