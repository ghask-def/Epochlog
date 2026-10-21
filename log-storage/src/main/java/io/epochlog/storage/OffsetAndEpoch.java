package io.epochlog.storage;

/**
 * An end offset paired with the epoch it belongs to, as returned by epoch lookups.
 *
 * <p>Thread-safety: immutable.
 */
public record OffsetAndEpoch(long offset, int epoch) {
    public static final int UNDEFINED_EPOCH = -1;
    public static final long UNDEFINED_OFFSET = -1L;
    public static final OffsetAndEpoch UNDEFINED = new OffsetAndEpoch(UNDEFINED_OFFSET, UNDEFINED_EPOCH);
}
