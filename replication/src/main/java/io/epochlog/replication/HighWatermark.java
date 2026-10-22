package io.epochlog.replication;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A partition's high watermark. On the leader it only ever moves forward: several threads
 * (request handlers, the writer, the group committer) compute candidates from slightly different
 * snapshots, and each candidate is a valid lower bound, so the largest one wins via CAS. Followers
 * and truncation set it outright.
 *
 * <p>Thread-safety: thread-safe (lock-free).
 */
public final class HighWatermark {
    private final AtomicLong value;

    public HighWatermark(long initial) {
        this.value = new AtomicLong(initial);
    }

    public long get() {
        return value.get();
    }

    /** Raises the watermark to {@code candidate} if that is higher. Returns true if it moved. */
    public boolean advanceTo(long candidate) {
        long current = value.get();
        while (candidate > current) {
            if (value.compareAndSet(current, candidate)) {
                return true;
            }
            current = value.get();
        }
        return false;
    }

    /** Overwrites the watermark. Only for followers and truncation, which own it outright. */
    public void set(long v) {
        value.set(v);
    }

    @Override
    public String toString() {
        return Long.toString(value.get());
    }
}
