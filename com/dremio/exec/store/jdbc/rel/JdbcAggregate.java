package com.dremio.exec.store.jdbc.rel;

import com.dremio.common.dialect.DremioSqlDialect;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.JdbcRelImpl;
import com.dremio.exec.store.jdbc.conf.DialectConf;
import com.dremio.exec.store.jdbc.dialect.JdbcDremioSqlDialect;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil.Suggester;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

public class JdbcAggregate extends Aggregate implements JdbcRelImpl {
   private final StoragePluginId pluginId;

   public JdbcAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls, StoragePluginId pluginId) throws InvalidRelException {
      super(cluster, traitSet, input, groupSet, groupSets, aggCalls);

      assert this.groupSets.size() == 1 : "Grouping sets not supported";

      this.pluginId = pluginId;
      Object dialect;
      if (null != pluginId && pluginId.getConnectionConf() instanceof DialectConf) {
         DialectConf<?, ?> conf = (DialectConf)pluginId.getConnectionConf();
         dialect = conf.getDialect();
      } else {
         dialect = DremioSqlDialect.CALCITE;
      }

      Iterator var11 = aggCalls.iterator();

      AggregateCall aggCall;
      do {
         if (!var11.hasNext()) {
            return;
         }

         aggCall = (AggregateCall)var11.next();
      } while(((DremioSqlDialect)dialect).supportsAggregateFunction(aggCall.getAggregation().getKind()));

      throw new InvalidRelException("cannot implement aggregate function " + aggCall.getAggregation());
   }

   public JdbcAggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
      try {
         return new JdbcAggregate(this.getCluster(), traitSet, input, groupSet, groupSets, aggCalls, this.pluginId);
      } catch (InvalidRelException var7) {
         throw new AssertionError(var7);
      }
   }

   public RelNode copyWith(CopyWithCluster copier) {
      RelNode input = this.getInput().accept(copier);

      try {
         return new JdbcAggregate(copier.getCluster(), this.getTraitSet(), input, this.getGroupSet(), this.getGroupSets(), copier.copyOf(this.getAggCallList()), this.pluginId);
      } catch (InvalidRelException var4) {
         throw new AssertionError(var4);
      }
   }

   public StoragePluginId getPluginId() {
      return this.pluginId;
   }

   public RelNode revert(List<RelNode> revertedInputs, RelBuilder builder) {
      return builder.push((RelNode)revertedInputs.get(0)).aggregate(builder.groupKey(this.groupSet, this.groupSets), this.aggCalls).build();
   }

   public RelNode shortenAliases(Suggester suggester, Set<String> usedAliases) {
      if (this.pluginId == null) {
         return this;
      } else {
         DialectConf<?, ?> conf = (DialectConf)this.pluginId.getConnectionConf();
         JdbcDremioSqlDialect dialect = conf.getDialect();
         if (dialect.getIdentifierLengthLimit() == null) {
            return this;
         } else {
            boolean needsShortenedAlias = false;
            Builder<AggregateCall> aggCallBuilder = ImmutableList.builder();
            Iterator var7 = this.aggCalls.iterator();

            while(true) {
               while(var7.hasNext()) {
                  AggregateCall aggCall = (AggregateCall)var7.next();
                  if (aggCall.getName() != null && aggCall.getName().length() > dialect.getIdentifierLengthLimit()) {
                     needsShortenedAlias = true;
                     String newAlias = SqlValidatorUtil.uniquify((String)null, usedAliases, suggester);
                     aggCallBuilder.add(aggCall.rename(newAlias));
                  } else {
                     aggCallBuilder.add(aggCall);
                  }
               }

               if (!needsShortenedAlias) {
                  return this;
               }

               return this.copy(this.getTraitSet(), this.input, this.getGroupSet(), this.getGroupSets(), aggCallBuilder.build());
            }
         }
      }
   }
}
