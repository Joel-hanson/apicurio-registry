package io.apicurio.examples.snapshotbench;

import org.h2.tools.Restore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Local micro-benchmark for KafkaSQL-style H2 snapshot formats.
 *
 * <p>Strategies:
 * <ol>
 *   <li><b>current</b> — {@code SCRIPT TO} / {@code RUNSCRIPT FROM} (what Apicurio uses today)</li>
 *   <li><b>gzip-script</b> — same SQL dump with H2 {@code COMPRESSION GZIP}</li>
 *   <li><b>h2-backup</b> — file-based H2 + {@code BACKUP TO} / {@link Restore}</li>
 * </ol>
 *
 * <p>This intentionally does <em>not</em> start Kafka or Registry. It isolates dump/restore cost
 * for registry-like rows (metadata + large schema content blobs).
 *
 * <p>Env / system properties (all optional):
 * <ul>
 *   <li>{@code ROWS} / {@code snapshot.bench.rows} — number of content rows (default 2000)</li>
 *   <li>{@code CONTENT_BYTES} / {@code snapshot.bench.contentBytes} — payload size per row (default 8192)</li>
 *   <li>{@code WARMUP} / {@code snapshot.bench.warmup} — warmup iterations (default 1)</li>
 *   <li>{@code ITERATIONS} / {@code snapshot.bench.iterations} — timed iterations (default 3)</li>
 *   <li>{@code WORK_DIR} / {@code snapshot.bench.workDir} — working directory (default ./bench-work)</li>
 * </ul>
 */
public final class SnapshotFormatBenchmark {

    private SnapshotFormatBenchmark() {
    }

    public static void main(String[] args) throws Exception {
        int rows = intProp("ROWS", "snapshot.bench.rows", 2000);
        int contentBytes = intProp("CONTENT_BYTES", "snapshot.bench.contentBytes", 8192);
        int warmup = intProp("WARMUP", "snapshot.bench.warmup", 1);
        int iterations = intProp("ITERATIONS", "snapshot.bench.iterations", 3);
        Path workDir = Path.of(prop("WORK_DIR", "snapshot.bench.workDir", "bench-work"))
                .toAbsolutePath().normalize();

        Files.createDirectories(workDir);
        String payload = buildPayload(contentBytes);

        System.out.println("=== KafkaSQL H2 snapshot format benchmark ===");
        System.out.printf(Locale.ROOT,
                "rows=%d contentBytes=%d (~%.1f MiB logical content) warmup=%d iterations=%d%n",
                rows, contentBytes, (rows * (double) contentBytes) / (1024 * 1024), warmup, iterations);
        System.out.println("workDir=" + workDir);
        System.out.println();

        List<StrategyResult> results = new ArrayList<>();
        results.add(runStrategy(new PlainScriptStrategy(), rows, payload, workDir, warmup, iterations));
        results.add(runStrategy(new GzipScriptStrategy(), rows, payload, workDir, warmup, iterations));
        results.add(runStrategy(new H2BackupStrategy(), rows, payload, workDir, warmup, iterations));

        printReport(results);
    }

    private static StrategyResult runStrategy(SnapshotStrategy strategy, int rows, String payload,
            Path workDir, int warmup, int iterations) throws Exception {
        System.out.println("--- " + strategy.name() + " ---");
        System.out.println(strategy.description());

        List<Long> createMs = new ArrayList<>();
        List<Long> restoreMs = new ArrayList<>();
        long snapshotBytes = -1;
        int verifiedRows = -1;

        int total = warmup + iterations;
        for (int i = 0; i < total; i++) {
            Path runDir = workDir.resolve(strategy.id() + "-run-" + i);
            deleteRecursively(runDir);
            Files.createDirectories(runDir);

            long createStart = System.nanoTime();
            Path snapshot = strategy.createSnapshot(runDir, rows, payload);
            long createElapsed = elapsedMs(createStart);

            long restoreStart = System.nanoTime();
            int count = strategy.restoreAndCount(runDir, snapshot);
            long restoreElapsed = elapsedMs(restoreStart);

            if (i >= warmup) {
                createMs.add(createElapsed);
                restoreMs.add(restoreElapsed);
                snapshotBytes = Files.size(snapshot);
                verifiedRows = count;
            }

            System.out.printf(Locale.ROOT,
                    "  iter %d/%d%s create=%d ms restore=%d ms file=%s rows=%d%n",
                    i + 1, total, i < warmup ? " (warmup)" : "",
                    createElapsed, restoreElapsed, humanBytes(Files.size(snapshot)), count);

            if (count != rows) {
                throw new IllegalStateException(strategy.name() + " restored " + count
                        + " rows, expected " + rows);
            }
        }

        StrategyResult result = new StrategyResult(strategy.name(), strategy.id(),
                avg(createMs), avg(restoreMs), snapshotBytes, verifiedRows);
        System.out.printf(Locale.ROOT, "  AVG create=%.1f ms restore=%.1f ms file=%s%n%n",
                result.avgCreateMs, result.avgRestoreMs, humanBytes(result.snapshotBytes));
        return result;
    }

    private static void printReport(List<StrategyResult> results) {
        StrategyResult baseline = results.stream()
                .filter(r -> "current".equals(r.id))
                .findFirst()
                .orElse(results.get(0));

        System.out.println("=== Summary (avg over timed iterations) ===");
        System.out.printf(Locale.ROOT, "%-18s %12s %12s %12s %10s %10s%n",
                "strategy", "create_ms", "restore_ms", "total_ms", "file", "vs_cur");
        System.out.println("-".repeat(78));

        for (StrategyResult r : results) {
            double total = r.avgCreateMs + r.avgRestoreMs;
            double baselineTotal = baseline.avgCreateMs + baseline.avgRestoreMs;
            double speedup = baselineTotal / total;
            System.out.printf(Locale.ROOT, "%-18s %12.1f %12.1f %12.1f %10s %9.2fx%n",
                    r.id, r.avgCreateMs, r.avgRestoreMs, total,
                    humanBytes(r.snapshotBytes), speedup);
        }

        System.out.println();
        System.out.println("Notes:");
        System.out.println("  - 'vs_cur' is baseline_total / strategy_total (create+restore). Higher is faster.");
        System.out.println("  - File size matters for /tmp and shared PVC pressure.");
        System.out.println("  - This isolates H2 dump/restore only; Kafka journal replay is separate.");
        System.out.println("  - h2-backup requires file-based H2 (not today's jdbc:h2:mem KafkaSQL URL).");
    }

    // --- strategies ---

    private interface SnapshotStrategy {
        String id();

        String name();

        String description();

        Path createSnapshot(Path runDir, int rows, String payload) throws Exception;

        int restoreAndCount(Path runDir, Path snapshot) throws Exception;
    }

    /** Mirrors Apicurio H2SqlStatements: SCRIPT TO / RUNSCRIPT FROM on an in-memory DB. */
    private static final class PlainScriptStrategy implements SnapshotStrategy {
        @Override
        public String id() {
            return "current";
        }

        @Override
        public String name() {
            return "1) Current SCRIPT (uncompressed)";
        }

        @Override
        public String description() {
            return "Same shape as KafkaSQL today: SCRIPT TO file.sql then RUNSCRIPT FROM file.sql";
        }

        @Override
        public Path createSnapshot(Path runDir, int rows, String payload) throws Exception {
            Path snapshot = runDir.resolve("snapshot.sql");
            String url = "jdbc:h2:mem:plain-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1";
            try (Connection conn = DriverManager.getConnection(url, "sa", "")) {
                seed(conn, rows, payload);
                try (PreparedStatement ps = conn.prepareStatement("SCRIPT TO ?")) {
                    ps.setString(1, snapshot.toString());
                    ps.executeQuery().close();
                }
            }
            return snapshot;
        }

        @Override
        public int restoreAndCount(Path runDir, Path snapshot) throws Exception {
            String url = "jdbc:h2:mem:plain-restore-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1";
            try (Connection conn = DriverManager.getConnection(url, "sa", "")) {
                try (PreparedStatement ps = conn.prepareStatement("RUNSCRIPT FROM ?")) {
                    ps.setString(1, snapshot.toString());
                    ps.executeUpdate();
                }
                return countContent(conn);
            }
        }
    }

    /** Option 1: compressed SQL script. */
    private static final class GzipScriptStrategy implements SnapshotStrategy {
        @Override
        public String id() {
            return "gzip-script";
        }

        @Override
        public String name() {
            return "2) SCRIPT + GZIP compression";
        }

        @Override
        public String description() {
            return "SCRIPT TO ? COMPRESSION GZIP / RUNSCRIPT FROM ? COMPRESSION GZIP";
        }

        @Override
        public Path createSnapshot(Path runDir, int rows, String payload) throws Exception {
            Path snapshot = runDir.resolve("snapshot.sql.gz");
            String url = "jdbc:h2:mem:gzip-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1";
            try (Connection conn = DriverManager.getConnection(url, "sa", "")) {
                seed(conn, rows, payload);
                try (PreparedStatement ps = conn.prepareStatement("SCRIPT TO ? COMPRESSION GZIP")) {
                    ps.setString(1, snapshot.toString());
                    ps.executeQuery().close();
                }
            }
            return snapshot;
        }

        @Override
        public int restoreAndCount(Path runDir, Path snapshot) throws Exception {
            String url = "jdbc:h2:mem:gzip-restore-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1";
            try (Connection conn = DriverManager.getConnection(url, "sa", "")) {
                try (PreparedStatement ps = conn.prepareStatement("RUNSCRIPT FROM ? COMPRESSION GZIP")) {
                    ps.setString(1, snapshot.toString());
                    ps.executeUpdate();
                }
                return countContent(conn);
            }
        }
    }

    /**
     * Option 2: native H2 binary backup. Requires a file-backed database (not mem).
     * Restore uses {@link Restore} into a fresh directory, then opens that DB.
     */
    private static final class H2BackupStrategy implements SnapshotStrategy {
        @Override
        public String id() {
            return "h2-backup";
        }

        @Override
        public String name() {
            return "3) H2 BACKUP TO (file DB)";
        }

        @Override
        public String description() {
            return "File-based H2 + BACKUP TO zip + Restore tool (closer to loading DB pages)";
        }

        @Override
        public Path createSnapshot(Path runDir, int rows, String payload) throws Exception {
            Path dbDir = runDir.resolve("db-src");
            Files.createDirectories(dbDir);
            Path dbPath = dbDir.resolve("registry");
            Path snapshot = runDir.resolve("snapshot.zip");

            String url = "jdbc:h2:file:" + dbPath.toAbsolutePath() + ";DB_CLOSE_DELAY=0";
            try (Connection conn = DriverManager.getConnection(url, "sa", "")) {
                seed(conn, rows, payload);
                try (PreparedStatement ps = conn.prepareStatement("BACKUP TO ?")) {
                    ps.setString(1, snapshot.toString());
                    ps.executeUpdate();
                }
            }
            return snapshot;
        }

        @Override
        public int restoreAndCount(Path runDir, Path snapshot) throws Exception {
            Path restoreDir = runDir.resolve("db-restored");
            Files.createDirectories(restoreDir);
            // Restore extracts DB files named "registry.*" into restoreDir
            Restore.execute(snapshot.toString(), restoreDir.toString(), "registry");

            Path dbPath = restoreDir.resolve("registry");
            String url = "jdbc:h2:file:" + dbPath.toAbsolutePath() + ";DB_CLOSE_DELAY=0";
            try (Connection conn = DriverManager.getConnection(url, "sa", "")) {
                return countContent(conn);
            }
        }
    }

    // --- schema + helpers ---

    /**
     * Minimal registry-like shape: content blobs + artifact metadata referencing them.
     * Not a full Apicurio schema — enough to stress snapshot formats with large text.
     */
    private static void seed(Connection conn, int rows, String payload) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("""
                    CREATE TABLE content (
                      content_id BIGINT PRIMARY KEY,
                      content_hash VARCHAR(128) NOT NULL,
                      canonical_hash VARCHAR(128),
                      content_type VARCHAR(255) NOT NULL,
                      content CLOB NOT NULL
                    )
                    """);
            st.execute("""
                    CREATE TABLE artifact (
                      group_id VARCHAR(512) NOT NULL,
                      artifact_id VARCHAR(512) NOT NULL,
                      artifact_type VARCHAR(64) NOT NULL,
                      content_id BIGINT NOT NULL,
                      PRIMARY KEY (group_id, artifact_id)
                    )
                    """);
            st.execute("CREATE INDEX idx_content_hash ON content(content_hash)");
        }

        try (PreparedStatement contentPs = conn.prepareStatement(
                "INSERT INTO content(content_id, content_hash, canonical_hash, content_type, content) VALUES (?,?,?,?,?)");
                PreparedStatement artifactPs = conn.prepareStatement(
                        "INSERT INTO artifact(group_id, artifact_id, artifact_type, content_id) VALUES (?,?,?,?)")) {
            for (int i = 0; i < rows; i++) {
                String hash = "hash-" + i;
                contentPs.setLong(1, i);
                contentPs.setString(2, hash);
                contentPs.setString(3, hash);
                contentPs.setString(4, "application/json");
                contentPs.setString(5, payload);
                contentPs.addBatch();

                artifactPs.setString(1, "default");
                artifactPs.setString(2, "artifact-" + i);
                artifactPs.setString(3, "JSON");
                artifactPs.setLong(4, i);
                artifactPs.addBatch();

                if (i % 500 == 0) {
                    contentPs.executeBatch();
                    artifactPs.executeBatch();
                }
            }
            contentPs.executeBatch();
            artifactPs.executeBatch();
        }
    }

    private static int countContent(Connection conn) throws Exception {
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM content")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private static String buildPayload(int bytes) {
        // Compressible schema-like JSON so GZIP results look realistic for OpenAPI/JSON Schema text.
        String unit = "{\"type\":\"object\",\"properties\":{\"field\":{\"type\":\"string\",\"description\":\"x\"}},";
        StringBuilder sb = new StringBuilder(bytes + 64);
        sb.append("{\"openapi\":\"3.0.3\",\"info\":{\"title\":\"bench\",\"version\":\"1\"},\"paths\":{},\"components\":{\"schemas\":{");
        while (sb.length() < bytes) {
            sb.append(unit);
        }
        sb.setLength(bytes);
        return sb.toString();
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private static long elapsedMs(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    private static double avg(List<Long> values) {
        return values.stream().mapToLong(Long::longValue).average().orElse(0);
    }

    private static String humanBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + "B";
        }
        if (bytes < 1024 * 1024) {
            return String.format(Locale.ROOT, "%.1fKiB", bytes / 1024.0);
        }
        return String.format(Locale.ROOT, "%.1fMiB", bytes / (1024.0 * 1024.0));
    }

    private static int intProp(String env, String sysProp, int defaultValue) {
        return Integer.parseInt(prop(env, sysProp, Integer.toString(defaultValue)));
    }

    private static String prop(String env, String sysProp, String defaultValue) {
        String fromEnv = System.getenv(env);
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv.trim();
        }
        return System.getProperty(sysProp, defaultValue);
    }

    private record StrategyResult(String name, String id, double avgCreateMs, double avgRestoreMs,
            long snapshotBytes, int rows) {
    }
}
