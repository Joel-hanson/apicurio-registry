package io.apicurio.registry.storage.impl.kafkasql;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaSnapshotStoreTest {

    @Test
    void roundTripChunks() throws Exception {
        Path dump = Files.createTempFile("snap-", ".sql.gz");
        try {
            byte[] payload = ("gzip-like-bytes-" + "x".repeat(2000)).getBytes(StandardCharsets.UTF_8);
            Files.write(dump, payload);

            List<String> chunks = KafkaSnapshotStore.readFileAsBase64Chunks(dump, 512);
            assertTrue(chunks.size() > 1);

            String snapshotId = "abc-123";
            List<KafkaSnapshotStore.KeyedValue> records = new ArrayList<>();
            for (int i = 0; i < chunks.size(); i++) {
                records.add(new KafkaSnapshotStore.KeyedValue(KafkaSnapshotStore.chunkKey(snapshotId, i),
                        chunks.get(i)));
            }
            Map<String, Map<Integer, String>> indexed = KafkaSnapshotStore.indexChunks(records);
            assertTrue(KafkaSnapshotStore.hasAllChunks(indexed.get(snapshotId), chunks.size()));

            Path materialized = KafkaSnapshotStore.materializeToTempFile(snapshotId,
                    indexed.get(snapshotId), chunks.size());
            try {
                assertArrayEquals(payload, Files.readAllBytes(materialized));
            } finally {
                Files.deleteIfExists(materialized);
            }
        } finally {
            Files.deleteIfExists(dump);
        }
    }

    @Test
    void incompleteChunksDetected() {
        Map<Integer, String> chunks = new HashMap<>();
        chunks.put(0, "AA==");
        assertEquals(false, KafkaSnapshotStore.hasAllChunks(chunks, 2));
    }

    @Test
    void selectObsoleteKeepsNewestRetainCount() {
        List<KafkaSnapshotStore.PublishedKafkaSnapshot> published = List.of(
                new KafkaSnapshotStore.PublishedKafkaSnapshot("old", 1L, 3),
                new KafkaSnapshotStore.PublishedKafkaSnapshot("mid", 2L, 2),
                new KafkaSnapshotStore.PublishedKafkaSnapshot("new", 3L, 4));

        List<KafkaSnapshotStore.PublishedKafkaSnapshot> obsolete = KafkaSnapshotStore
                .selectObsoleteKafkaSnapshots(published, 2);
        assertEquals(1, obsolete.size());
        assertEquals("old", obsolete.get(0).snapshotId());

        List<String> keys = KafkaSnapshotStore.tombstoneKeys(obsolete.get(0));
        assertEquals(List.of("old", "old/chunk/0", "old/chunk/1", "old/chunk/2"), keys);
    }

    @Test
    void selectObsoleteRetainOne() {
        List<KafkaSnapshotStore.PublishedKafkaSnapshot> published = List.of(
                new KafkaSnapshotStore.PublishedKafkaSnapshot("a", 10L, 1),
                new KafkaSnapshotStore.PublishedKafkaSnapshot("b", 20L, 1));
        List<KafkaSnapshotStore.PublishedKafkaSnapshot> obsolete = KafkaSnapshotStore
                .selectObsoleteKafkaSnapshots(published, 1);
        assertEquals(1, obsolete.size());
        assertEquals("a", obsolete.get(0).snapshotId());
    }

    @Test
    void selectObsoleteNoOpWhenUnderRetainCount() {
        List<KafkaSnapshotStore.PublishedKafkaSnapshot> published = List.of(
                new KafkaSnapshotStore.PublishedKafkaSnapshot("only", 1L, 1));
        assertTrue(KafkaSnapshotStore.selectObsoleteKafkaSnapshots(published, 2).isEmpty());
    }
}
