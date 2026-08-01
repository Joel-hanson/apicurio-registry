package io.apicurio.registry.storage.impl.kafkasql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helpers for storing H2 snapshot dumps as chunked Base64 records on the snapshots topic so pods can
 * restore without a durable local filesystem (e.g. ephemeral /tmp).
 */
final class KafkaSnapshotStore {

    private static final Logger log = LoggerFactory.getLogger(KafkaSnapshotStore.class);

    static final String STORAGE_KAFKA = "kafka";
    static final String STORAGE_FILESYSTEM = "filesystem";

    private static final Pattern CHUNK_KEY = Pattern.compile("^([^/]+)/chunk/(\\d+)$");

    private KafkaSnapshotStore() {
    }

    static String chunkKey(String snapshotId, int index) {
        return snapshotId + "/chunk/" + index;
    }

    /**
     * Split a dump file into Base64 chunk payloads sized for default Kafka message limits.
     */
    static List<String> readFileAsBase64Chunks(Path dumpFile, int chunkBytes) throws IOException {
        if (chunkBytes < 1024) {
            throw new IllegalArgumentException("chunkBytes must be >= 1024");
        }
        byte[] all = Files.readAllBytes(dumpFile);
        List<String> chunks = new ArrayList<>();
        Base64.Encoder encoder = Base64.getEncoder();
        for (int offset = 0; offset < all.length; offset += chunkBytes) {
            int len = Math.min(chunkBytes, all.length - offset);
            chunks.add(encoder.encodeToString(java.util.Arrays.copyOfRange(all, offset, offset + len)));
        }
        // Empty dump still needs a restoreable file; publish one empty chunk.
        if (chunks.isEmpty()) {
            chunks.add(encoder.encodeToString(new byte[0]));
        }
        return chunks;
    }

    static boolean isChunkKey(String key) {
        return key != null && CHUNK_KEY.matcher(key).matches();
    }

    static ChunkRef parseChunkKey(String key) {
        if (key == null) {
            return null;
        }
        Matcher m = CHUNK_KEY.matcher(key);
        if (!m.matches()) {
            return null;
        }
        return new ChunkRef(m.group(1), Integer.parseInt(m.group(2)));
    }

    /**
     * Index chunk payloads from snapshot-topic records: snapshotId -> (index -> base64).
     */
    static Map<String, Map<Integer, String>> indexChunks(Iterable<KeyedValue> records) {
        Map<String, Map<Integer, String>> bySnapshot = new HashMap<>();
        for (KeyedValue record : records) {
            ChunkRef ref = parseChunkKey(record.key());
            if (ref == null || record.value() == null) {
                continue;
            }
            bySnapshot.computeIfAbsent(ref.snapshotId(), ignored -> new HashMap<>()).put(ref.index(),
                    record.value());
        }
        return bySnapshot;
    }

    static boolean hasAllChunks(Map<Integer, String> chunks, int expectedCount) {
        if (chunks == null || expectedCount <= 0) {
            return false;
        }
        for (int i = 0; i < expectedCount; i++) {
            if (!chunks.containsKey(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reassemble Base64 chunks into a temp .sql.gz file for H2 RUNSCRIPT.
     */
    static Path materializeToTempFile(String snapshotId, Map<Integer, String> chunks, int chunkCount)
            throws IOException {
        if (!hasAllChunks(chunks, chunkCount)) {
            throw new IOException("Incomplete Kafka snapshot chunks for " + snapshotId + " (have "
                    + (chunks == null ? 0 : chunks.size()) + " of " + chunkCount + ")");
        }
        Path temp = Files.createTempFile("kafkasql-snap-" + snapshotId + "-", ".sql.gz");
        Base64.Decoder decoder = Base64.getDecoder();
        try (OutputStream out = Files.newOutputStream(temp)) {
            for (int i = 0; i < chunkCount; i++) {
                out.write(decoder.decode(chunks.get(i)));
            }
        } catch (RuntimeException | IOException e) {
            try {
                Files.deleteIfExists(temp);
            } catch (IOException ignored) {
                // best-effort cleanup
            }
            throw e;
        }
        log.info("Materialized Kafka-resident snapshot {} ({} chunks, {} bytes) to {}", snapshotId,
                chunkCount, Files.size(temp), temp);
        return temp;
    }

    static boolean isKafkaStorage(SnapshotMetadata metadata) {
        if (metadata == null || metadata.getStorage() == null) {
            return false;
        }
        return STORAGE_KAFKA.equals(metadata.getStorage().toLowerCase(Locale.ROOT));
    }

    record ChunkRef(String snapshotId, int index) {
    }

    record KeyedValue(String key, String value) {
    }
}
