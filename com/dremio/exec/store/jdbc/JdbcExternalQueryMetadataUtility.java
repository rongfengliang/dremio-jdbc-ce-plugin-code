package com.dremio.exec.store.jdbc;

import com.dremio.common.concurrent.NamedThreadFactory;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.GetExternalQueryMetadataRequest;
import com.dremio.exec.store.jdbc.JdbcFetcherProto.GetExternalQueryMetadataResponse;
import com.dremio.exec.store.jdbc.dialect.JdbcDremioSqlDialect;
import com.dremio.exec.store.jdbc.dialect.JdbcToFieldMapping;
import com.dremio.exec.store.jdbc.dialect.TypeMapper;
import com.dremio.exec.tablefunctions.DremioCalciteResource;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.Resources.ExInst;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JdbcExternalQueryMetadataUtility {
   private static final Logger LOGGER = LoggerFactory.getLogger(JdbcExternalQueryMetadataUtility.class);
   private static final long METADATA_RETRIEVAL_TIMEOUT_MILLISECONDS;

   private JdbcExternalQueryMetadataUtility() {
   }

   public static GetExternalQueryMetadataResponse getBatchSchema(DataSource source, JdbcDremioSqlDialect dialect, GetExternalQueryMetadataRequest request, JdbcPluginConfig config) {
      ExecutorService executor = Executors.newSingleThreadExecutor(new NamedThreadFactory(Thread.currentThread().getName() + ":jdbc-eq-metadata"));
      Callable<GetExternalQueryMetadataResponse> retrieveMetadata = () -> {
         return getExternalQueryMetadataFromSource(source, dialect, request, config);
      };
      Future<GetExternalQueryMetadataResponse> future = executor.submit(retrieveMetadata);
      return handleMetadataFuture(future, executor, METADATA_RETRIEVAL_TIMEOUT_MILLISECONDS);
   }

   static GetExternalQueryMetadataResponse handleMetadataFuture(Future<GetExternalQueryMetadataResponse> future, ExecutorService executor, long timeout) {
      GetExternalQueryMetadataResponse var4;
      try {
         var4 = (GetExternalQueryMetadataResponse)future.get(timeout, TimeUnit.MILLISECONDS);
      } catch (TimeoutException var11) {
         LOGGER.debug("Timeout while fetching metadata", var11);
         throw newValidationError(DremioCalciteResource.DREMIO_CALCITE_RESOURCE.externalQueryMetadataRetrievalError(var11));
      } catch (InterruptedException var12) {
         Thread.currentThread().interrupt();
         throw newValidationError(DremioCalciteResource.DREMIO_CALCITE_RESOURCE.externalQueryMetadataRetrievalError(var12));
      } catch (ExecutionException var13) {
         Throwable cause = var13.getCause();
         if (cause instanceof CalciteContextException) {
            throw (CalciteContextException)cause;
         }

         throw newValidationError(DremioCalciteResource.DREMIO_CALCITE_RESOURCE.externalQueryMetadataRetrievalError(cause));
      } finally {
         future.cancel(true);
         executor.shutdownNow();
      }

      return var4;
   }

   private static GetExternalQueryMetadataResponse getExternalQueryMetadataFromSource(DataSource source, JdbcDremioSqlDialect dialect, GetExternalQueryMetadataRequest request, JdbcPluginConfig config) throws SQLException {
      Connection conn = source.getConnection();
      Throwable var5 = null;

      GetExternalQueryMetadataResponse var11;
      try {
         PreparedStatement stmt = conn.prepareStatement(request.getSql());
         Throwable var7 = null;

         try {
            stmt.setQueryTimeout(Ints.saturatedCast(TimeUnit.MILLISECONDS.toSeconds(METADATA_RETRIEVAL_TIMEOUT_MILLISECONDS)));
            ResultSetMetaData metaData = stmt.getMetaData();
            if (metaData == null) {
               throw newValidationError(DremioCalciteResource.DREMIO_CALCITE_RESOURCE.externalQueryInvalidError(config.getSourceName()));
            }

            List<JdbcToFieldMapping> mappings = dialect.getDataTypeMapper(config).mapJdbcToArrowFields((TypeMapper.UnrecognizedTypeMarker)null, (TypeMapper.AddPropertyCallback)null, (TypeMapper.InvalidMetaDataCallback)((message) -> {
               throw UserException.invalidMetadataError().addContext(message).buildSilently();
            }), (Connection)conn, (ResultSetMetaData)metaData, (Set)null, true);
            ByteString bytes = ByteString.copyFrom(BatchSchema.newBuilder().addFields((Iterable)mappings.stream().map(JdbcToFieldMapping::getField).collect(Collectors.toList())).build().serialize());
            var11 = GetExternalQueryMetadataResponse.newBuilder().setBatchSchema(bytes).build();
         } catch (Throwable var22) {
            var7 = var22;
            throw var22;
         } finally {
            if (stmt != null) {
               $closeResource(var7, stmt);
            }

         }
      } catch (Throwable var24) {
         var5 = var24;
         throw var24;
      } finally {
         if (conn != null) {
            $closeResource(var5, conn);
         }

      }

      return var11;
   }

   @VisibleForTesting
   static CalciteContextException newValidationError(ExInst<SqlValidatorException> exceptionExInst) {
      return SqlUtil.newContextException(SqlParserPos.ZERO, exceptionExInst);
   }

   // $FF: synthetic method
   private static void $closeResource(Throwable x0, AutoCloseable x1) {
      if (x0 != null) {
         try {
            x1.close();
         } catch (Throwable var3) {
            x0.addSuppressed(var3);
         }
      } else {
         x1.close();
      }

   }

   static {
      METADATA_RETRIEVAL_TIMEOUT_MILLISECONDS = TimeUnit.SECONDS.toMillis(120L);
   }
}
