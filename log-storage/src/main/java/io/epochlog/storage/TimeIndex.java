package io.epochlog.storage;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;

/**
 * Sparse map from HLC timestamp to offset within one segment. Entries are
 * {@code (int64 timestamp, int32 relativeOffset)} where the timestamp is the base timestamp of
 * the batch starting at that offset. Leaders stamp batches from a hybrid logical clock, so
 * timestamps never go backwards within a partition and the index stays sorted on both keys.
 *
 * <p>Thread-safety: single-writer, concurrent readers. See {@link AbstractIndex}.
 */
public final class TimeIndex extends AbstractIndex {
    static final int ENTRY_SIZE = 12;

    /**
     * An index hit.
     *
     * <p>Thread-safety: immutable.
     */
    public record TimestampOffset(long timestamp, long offset) {}

    public TimeIndex(Path file, long baseOffset, int maxIndexBytes, boolean writable) throws IOException {
        super(file, baseOffset, maxIndexBytes, writable, ENTRY_SIZE);
    }

    /** Adds an entry unless the timestamp is not newer than the last one. */
    public void maybeAppend(long timestamp, long offset) {
        if (timestamp < 0 || isFull()) {
            return;
        }
        int n = entries();
        MappedByteBuffer buf = buffer();
        if (n > 0 && (timestamp <= keyAt(buf, n - 1) || offset <= offsetAt(buf, n - 1))) {
            return;
        }
        buf.putLong(n * ENTRY_SIZE, timestamp);
        buf.putInt(n * ENTRY_SIZE + 8, relativeOffset(offset));
        setEntries(n + 1);
    }

    /** Latest entry with timestamp {@code <= target}; the segment start if none. */
    public TimestampOffset lookup(long target) {
        int n = entries();
        MappedByteBuffer buf = buffer();
        int slot = largestLowerBound(target, n, buf);
        if (slot < 0) {
            return new TimestampOffset(-1L, baseOffset);
        }
        return new TimestampOffset(keyAt(buf, slot), offsetAt(buf, slot));
    }

    public long lastTimestamp() {
        int n = entries();
        return n == 0 ? -1L : keyAt(buffer(), n - 1);
    }

    @Override
    protected long keyAt(MappedByteBuffer buf, int slot) {
        return buf.getLong(slot * ENTRY_SIZE);
    }

    @Override
    protected long offsetAt(MappedByteBuffer buf, int slot) {
        return baseOffset + buf.getInt(slot * ENTRY_SIZE + 8);
    }
}
