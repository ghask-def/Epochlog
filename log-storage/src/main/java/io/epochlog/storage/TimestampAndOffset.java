package io.epochlog.storage;

/**
 * Result of a timestamp lookup: the first record at or after the target time.
 *
 * <p>Thread-safety: immutable.
 */
public record TimestampAndOffset(long timestamp, long offset) {}
