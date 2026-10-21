package io.epochlog.storage;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;

/**
 * Sparse map from offset to file position within one segment. Entries are
 * {@code (int32 relativeOffset, int32 position)}, one per {@code index.interval.bytes} of log.
 *
 * <p>Thread-safety: single-writer, concurrent readers. See {@link AbstractIndex}.
 */
public final class OffsetIndex extends AbstractIndex {
    static final int ENTRY_SIZE = 8;

    /**
     * An index hit.
     *
     * <p>Thread-safety: immutable.
     */
    public record OffsetPosition(long offset, int position) {}

    public OffsetIndex(Path file, long baseOffset, int maxIndexBytes, boolean writable) throws IOException {
        super(file, baseOffset, maxIndexBytes, writable, ENTRY_SIZE);
    }

    public void append(long offset, int position) {
        int n = entries();
        if (isFull()) {
            throw new IllegalStateException("offset index full: " + file());
        }
        if (n > 0 && offset <= lastOffset()) {
            throw new IllegalArgumentException("offset " + offset + " not after last indexed " + lastOffset());
        }
        MappedByteBuffer buf = buffer();
        buf.putInt(n * ENTRY_SIZE, relativeOffset(offset));
        buf.putInt(n * ENTRY_SIZE + 4, position);
        setEntries(n + 1);
    }

    /** Closest indexed position at or before {@code target}; the segment start if none. */
    public OffsetPosition lookup(long target) {
        int n = entries();
        MappedByteBuffer buf = buffer();
        int slot = largestLowerBound(target, n, buf);
        if (slot < 0) {
            return new OffsetPosition(baseOffset, 0);
        }
        return new OffsetPosition(offsetAt(buf, slot), buf.getInt(slot * ENTRY_SIZE + 4));
    }

    public long lastOffset() {
        int n = entries();
        return n == 0 ? baseOffset : offsetAt(buffer(), n - 1);
    }

    public OffsetPosition lastEntry() {
        int n = entries();
        if (n == 0) {
            return new OffsetPosition(baseOffset, 0);
        }
        MappedByteBuffer buf = buffer();
        return new OffsetPosition(offsetAt(buf, n - 1), buf.getInt((n - 1) * ENTRY_SIZE + 4));
    }

    /** True if entries are strictly increasing; used to decide whether a reopened index is trustworthy. */
    public boolean sanityCheck(long segmentSizeBytes) {
        int n = entries();
        MappedByteBuffer buf = buffer();
        long prevOffset = -1;
        int prevPos = -1;
        for (int i = 0; i < n; i++) {
            long off = offsetAt(buf, i);
            int pos = buf.getInt(i * ENTRY_SIZE + 4);
            if (off <= prevOffset || pos <= prevPos || pos >= segmentSizeBytes) {
                return false;
            }
            prevOffset = off;
            prevPos = pos;
        }
        return true;
    }

    @Override
    protected long keyAt(MappedByteBuffer buf, int slot) {
        return offsetAt(buf, slot);
    }

    @Override
    protected long offsetAt(MappedByteBuffer buf, int slot) {
        return baseOffset + buf.getInt(slot * ENTRY_SIZE);
    }
}
