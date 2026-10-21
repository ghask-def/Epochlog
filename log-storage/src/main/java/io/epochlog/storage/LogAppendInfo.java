package io.epochlog.storage;

/**
 * Result of appending one batch.
 *
 * @param duplicate true if an idempotent producer retried a batch already in the log; the offsets
 *     then refer to the original copy.
 *
 * <p>Thread-safety: immutable.
 */
public record LogAppendInfo(long firstOffset, long lastOffset, long maxTimestamp, boolean duplicate) {}
