package io.apicurio.registry.storage.impl.sql;

import io.apicurio.registry.logging.Logged;
import io.apicurio.registry.metrics.StorageMetricsApply;
import io.apicurio.registry.metrics.health.liveness.PersistenceExceptionLivenessApply;
import io.apicurio.registry.metrics.health.readiness.PersistenceTimeoutReadinessApply;
import io.apicurio.registry.storage.RegistryStorage;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * An in-memory SQL implementation of the {@link RegistryStorage} interface.
 */
@ApplicationScoped
@PersistenceExceptionLivenessApply
@PersistenceTimeoutReadinessApply
@StorageMetricsApply
@Logged
public class SqlRegistryStorage extends AbstractSqlRegistryStorage {

    @Inject
    HandleFactory handleFactory;

    /**
     * @see io.apicurio.registry.storage.RegistryStorage#storageName()
     */
    @Override
    public String storageName() {
        return "sql";
    }

    @Override
    public void initialize() {
        initialize(handleFactory, true);
    }

    public void restoreFromSnapshot(String snapshotLocation) {
        // Prefer GZIP restore for .sql.gz dumps (current format). Fall back to uncompressed SCRIPT for
        // legacy .sql files left on disk from older Registry versions.
        final String sql;
        if (snapshotLocation != null && snapshotLocation.endsWith(".gz")) {
            sql = sqlStatements.restoreFromSnapshot();
        } else if ("h2".equals(sqlStatements.dbType())) {
            sql = "RUNSCRIPT FROM ?";
        } else {
            sql = sqlStatements.restoreFromSnapshot();
        }
        handleFactory.withHandle(handle -> handle.createUpdate(sql).bind(0, snapshotLocation).execute());
    }

    public void executeSqlStatement(String sqlStatement) {
        handleFactory.withHandle(handle -> handle.createUpdate(sqlStatement).execute());
    }
}
