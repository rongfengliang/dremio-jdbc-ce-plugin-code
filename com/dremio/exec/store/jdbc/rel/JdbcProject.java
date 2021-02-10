package com.dremio.exec.store.jdbc.rel;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.JdbcRelImpl;
import com.dremio.exec.planner.common.MoreRelOptUtil.ContainsRexVisitor;
import com.dremio.exec.store.jdbc.conf.DialectConf;
import com.dremio.exec.store.jdbc.legacy.JdbcDremioSqlDialect;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil.Suggester;
import org.apache.calcite.tools.RelBuilder;

public class JdbcProject extends Project implements JdbcRelImpl {
   private final boolean foundContains;
   private final StoragePluginId pluginId;
   private final boolean isDummy;

   public JdbcProject(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<? extends RexNode> projects, RelDataType rowType, StoragePluginId pluginId) {
      this(cluster, traitSet, input, projects, rowType, pluginId, false);
   }

   JdbcProject(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<? extends RexNode> projects, RelDataType rowType, StoragePluginId pluginId, boolean isDummy) {
      super(cluster, traitSet, input, projects, rowType);
      this.pluginId = pluginId;
      boolean foundContains = false;
      Iterator var9 = this.getChildExps().iterator();

      while(var9.hasNext()) {
         RexNode rex = (RexNode)var9.next();
         if (ContainsRexVisitor.hasContains(rex)) {
            foundContains = true;
            break;
         }
      }

      this.foundContains = foundContains;
      this.isDummy = isDummy;
   }

   public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
      if (this.foundContains) {
         return planner.getCostFactory().makeInfiniteCost();
      } else {
         double rowCount = mq.getRowCount(this);
         return planner.getCostFactory().makeCost(rowCount, 0.0D, 0.0D);
      }
   }

   public JdbcProject copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
      return new JdbcProject(this.getCluster(), traitSet, input, projects, rowType, this.pluginId);
   }

   public RelNode copyWith(CopyWithCluster copier) {
      RelNode input = this.getInput().accept(copier);
      return new JdbcProject(copier.getCluster(), this.getTraitSet(), input, copier.copyRexNodes(this.getProjects()), copier.copyOf(this.getRowType()), this.pluginId);
   }

   public StoragePluginId getPluginId() {
      return this.pluginId;
   }

   public Project revert(List<RelNode> revertedInputs, RelBuilder builder) {
      return (Project)builder.push((RelNode)revertedInputs.get(0)).projectNamed(this.getProjects(), this.getRowType().getFieldNames(), true).build();
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
            RelDataType types = this.getRowType();
            boolean needsShortenedAlias = false;
            Builder shortenedFieldBuilder = this.getCluster().getTypeFactory().builder();
            Iterator var8 = types.getFieldList().iterator();

            while(var8.hasNext()) {
               RelDataTypeField originalField = (RelDataTypeField)var8.next();
               if (originalField.getName().length() > dialect.getIdentifierLengthLimit()) {
                  needsShortenedAlias = true;
                  String newAlias = SqlValidatorUtil.uniquify((String)null, usedAliases, suggester);
                  shortenedFieldBuilder.add(newAlias, originalField.getType());
               } else {
                  shortenedFieldBuilder.add(originalField);
               }
            }

            if (!needsShortenedAlias) {
               return this;
            } else {
               return this.copy(this.traitSet, this.input, this.exps, shortenedFieldBuilder.build());
            }
         }
      }
   }

   public boolean isDummyProject() {
      return this.isDummy;
   }
}
