package io.apicurio.registry.storage.impl.kafkasql;

/**
 * Result of applying a CreateSnapshot journal message on the consumer thread. Carries the dump path plus the
 * journal coordinates of the marker so they can be published to the snapshots topic.
 */
public record SnapshotCreationResult(String snapshotLocation, String journalTopic, int journalPartition,
        long journalOffset) {
}
