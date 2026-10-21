package io.epochlog.common;

import java.time.Instant;

/**
 * Packing helpers for hybrid logical timestamps. The upper 48 bits hold wall-clock milliseconds,
 * the lower 16 bits a logical counter, so plain {@code long} comparison matches HLC ordering.
 *
 * <p>Thread-safety: thread-safe (stateless).
 */
public final class HybridTime {
    public static final int LOGICAL_BITS = 16;
    public static final long LOGICAL_MASK = (1L << LOGICAL_BITS) - 1;
    public static final long NO_TIMESTAMP = -1L;

    private HybridTime() {}

    public static long pack(long physicalMillis, int logical) {
        return (physicalMillis << LOGICAL_BITS) | (logical & LOGICAL_MASK);
    }

    public static long physicalMillis(long hlc) {
        return hlc >>> LOGICAL_BITS;
    }

    public static int logical(long hlc) {
        return (int) (hlc & LOGICAL_MASK);
    }

    /** The smallest HLC value whose physical part is {@code millis}. Handy for timestamp lookups. */
    public static long fromMillis(long millis) {
        return pack(millis, 0);
    }

    public static String format(long hlc) {
        if (hlc < 0) {
            return "none";
        }
        return Instant.ofEpochMilli(physicalMillis(hlc)) + "+" + logical(hlc);
    }
}
