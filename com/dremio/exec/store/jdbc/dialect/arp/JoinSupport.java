package com.dremio.exec.store.jdbc.dialect.arp;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.calcite.sql.JoinType;

public class JoinSupport extends RelationalAlgebraOperation {
   private final JoinOp crossJoin;
   private final JoinOpWithCondition innerJoin;
   private final JoinOpWithCondition rightJoin;
   private final JoinOpWithCondition leftJoin;
   private final JoinOpWithCondition fullOuterJoin;

   @JsonCreator
   JoinSupport(@JsonProperty("enable") boolean enable, @JsonProperty("cross") JoinOp crossJoin, @JsonProperty("inner") JoinOpWithCondition innerJoin, @JsonProperty("left") JoinOpWithCondition leftJoin, @JsonProperty("right") JoinOpWithCondition rightJoin, @JsonProperty("full") JoinOpWithCondition fullOuterJoin) {
      super(enable);
      this.crossJoin = crossJoin;
      this.innerJoin = innerJoin;
      this.rightJoin = rightJoin;
      this.leftJoin = leftJoin;
      this.fullOuterJoin = fullOuterJoin;
   }

   public JoinOp getJoinOp(JoinType type) {
      switch(type) {
      case COMMA:
      case CROSS:
         return this.crossJoin;
      case INNER:
         return this.innerJoin;
      case LEFT:
         return this.leftJoin;
      case RIGHT:
         return this.rightJoin;
      case FULL:
      default:
         return this.fullOuterJoin;
      }
   }
}
