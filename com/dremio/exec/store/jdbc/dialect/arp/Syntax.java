package com.dremio.exec.store.jdbc.dialect.arp;

import com.dremio.common.dialect.DremioSqlDialect.ContainerSupport;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Syntax {
   private final ContainerSupport supportsCatalogs;
   private final ContainerSupport supportsSchemas;
   private final boolean shouldInjectNumericCastToProject;
   private final boolean shouldInjectApproxNumericCastToProject;
   private final String identifierQuote;

   @JsonCreator
   public Syntax(@JsonProperty("identifier_quote") String identifierQuote, @JsonProperty("supports_catalogs") Boolean supportsCatalogs, @JsonProperty("supports_schemas") Boolean supportsSchemas, @JsonProperty("inject_numeric_cast_project") Boolean shouldInjectNumericCastToProject, @JsonProperty("inject_approx_numeric_cast_project") Boolean shouldInjectApproxNumericCastToProject) {
      this.identifierQuote = identifierQuote;
      this.supportsCatalogs = supportsCatalogs == null ? ContainerSupport.AUTO_DETECT : (supportsCatalogs ? ContainerSupport.SUPPORTED : ContainerSupport.UNSUPPORTED);
      this.supportsSchemas = supportsSchemas == null ? ContainerSupport.AUTO_DETECT : (supportsSchemas ? ContainerSupport.SUPPORTED : ContainerSupport.UNSUPPORTED);
      this.shouldInjectNumericCastToProject = shouldInjectNumericCastToProject == null ? false : shouldInjectNumericCastToProject;
      this.shouldInjectApproxNumericCastToProject = shouldInjectApproxNumericCastToProject == null ? false : shouldInjectApproxNumericCastToProject;
   }

   public String getIdentifierQuote() {
      return this.identifierQuote;
   }

   public ContainerSupport supportsCatalogs() {
      return this.supportsCatalogs;
   }

   public ContainerSupport supportsSchemas() {
      return this.supportsSchemas;
   }

   public boolean shouldInjectNumericCastToProject() {
      return this.shouldInjectNumericCastToProject;
   }

   public boolean shouldInjectApproxNumericCastToProject() {
      return this.shouldInjectApproxNumericCastToProject;
   }
}
