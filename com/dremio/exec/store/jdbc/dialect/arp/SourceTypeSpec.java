package com.dremio.exec.store.jdbc.dialect.arp;

import java.util.TimeZone;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

class SourceTypeSpec extends SqlDataTypeSpec {
   private final String sourceTypeName;
   private final Mapping.RequiredCastArgs castArgs;

   SourceTypeSpec(String sourceTypeName, Mapping.RequiredCastArgs castArgs, int precision, int scale) {
      super(new SqlIdentifier(sourceTypeName, SqlParserPos.ZERO), precision, scale, (String)null, (TimeZone)null, SqlParserPos.ZERO);
      this.sourceTypeName = sourceTypeName;
      this.castArgs = castArgs;
   }

   public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      boolean hasPrecision = this.getPrecision() != -1;
      boolean hasScale = this.getScale() != Integer.MIN_VALUE;
      Mapping.RequiredCastArgs args = Mapping.RequiredCastArgs.getRequiredArgsBasedOnInputs(hasPrecision, hasScale, this.castArgs);
      writer.print(args.serializeArguments(this.sourceTypeName, this.getPrecision(), this.getScale()));
   }
}
