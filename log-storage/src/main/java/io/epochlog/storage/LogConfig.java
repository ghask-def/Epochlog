package io.epochlog.storage;

import io.epochlog.common.Config;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Per-partition log settings.
 *
 * <p>Thread-safety: immutable.
 */
public record LogConfig(
        int segmentBytes,
        long segmentMs,
        int indexIntervalBytes,
        int maxIndexBytes,
        long retentionMs,
        long retentionBytes,
        boolean compact,
        int maxBatchBytes) {

    public static final String SEGMENT_BYTES = "segment.bytes";
    public static final String SEGMENT_MS = "segment.ms";
    public static final String INDEX_INTERVAL_BYTES = "index.interval.bytes";
    public static final String SEGMENT_INDEX_BYTES = "segment.index.bytes";
    public static final String RETENTION_MS = "retention.ms";
    public static final String RETENTION_BYTES = "retention.bytes";
    public static final String CLEANUP_POLICY = "cleanup.policy";
    public static final String MAX_MESSAGE_BYTES = "max.message.bytes";

    public LogConfig {
        if (segmentBytes < 1024) {
            throw new IllegalArgumentException("segment.bytes too small: " + segmentBytes);
        }
        if (maxIndexBytes < 64) {
            throw new IllegalArgumentException("segment.index.bytes too small: " + maxIndexBytes);
        }
    }

    public static LogConfig defaults() {
        return from(Config.empty());
    }

    public static LogConfig from(Config c) {
        return new LogConfig(
                c.getInt(SEGMENT_BYTES, 1 << 30),
                c.getDurationMs(SEGMENT_MS, 7L * 24 * 3_600_000L),
                c.getInt(INDEX_INTERVAL_BYTES, 4096),
                c.getInt(SEGMENT_INDEX_BYTES, 10 << 20),
                c.getDurationMs(RETENTION_MS, 7L * 24 * 3_600_000L),
                c.getLong(RETENTION_BYTES, -1L),
                "compact".equalsIgnoreCase(c.getString(CLEANUP_POLICY, "delete")),
                c.getInt(MAX_MESSAGE_BYTES, 1 << 20));
    }

    public Map<String, String> toMap() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(SEGMENT_BYTES, Integer.toString(segmentBytes));
        m.put(SEGMENT_MS, Long.toString(segmentMs));
        m.put(INDEX_INTERVAL_BYTES, Integer.toString(indexIntervalBytes));
        m.put(SEGMENT_INDEX_BYTES, Integer.toString(maxIndexBytes));
        m.put(RETENTION_MS, Long.toString(retentionMs));
        m.put(RETENTION_BYTES, Long.toString(retentionBytes));
        m.put(CLEANUP_POLICY, compact ? "compact" : "delete");
        m.put(MAX_MESSAGE_BYTES, Integer.toString(maxBatchBytes));
        return m;
    }
}
