package io.apicurio.registry.storage.impl.kafkasql;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Payload stored on the kafkasql-snapshots topic. Older Registry versions wrote a bare filesystem path;
 * newer versions write this JSON document so bootstrap can seek past the snapshot marker.
 */
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Getter
@Setter
@EqualsAndHashCode
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SnapshotMetadata {

    public static final int CURRENT_VERSION = 2;

    private int version;
    /** Local filesystem path of the dump (optional cache when storage is kafka). */
    private String path;
    /**
     * Where dump bytes live for restore: {@code filesystem} (default/legacy) or {@code kafka}
     * (chunked records on the snapshots topic).
     */
    private String storage;
    /** Number of {@code {snapshotId}/chunk/{i}} records when storage is kafka. */
    private Integer chunkCount;
    private JournalPosition journal;

    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @Getter
    @Setter
    @EqualsAndHashCode
    @ToString
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class JournalPosition {
        private String topic;
        private int partition;
        private long offset;
    }
}
