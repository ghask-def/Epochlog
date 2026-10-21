package io.epochlog.clock;

import io.epochlog.common.HybridTime;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hybrid logical clock (Kulkarni et al., 2014). Timestamps pack physical milliseconds and a
 * logical counter into one {@code long} (see {@link HybridTime}), so the paper's rules reduce to
 * taking a max over longs:
 *
 * <ul>
 *   <li>local or send event: {@code next = max(last + 1, pt << 16)}
 *   <li>receive event: {@code next = max(last + 1, remote + 1, pt << 16)}
 * </ul>
 *
 * If the logical counter overflows it carries into the physical part, so the clock runs a
 * millisecond ahead rather than breaking monotonicity. Remote timestamps more than
 * {@code maxDriftMs} ahead of local physical time are rejected; letting them in would drag
 * every later timestamp forward with them.
 *
 * <p>Thread-safety: thread-safe. State is a single {@link AtomicLong} updated with a CAS loop.
 */
public final class HybridLogicalClock {
    private static final Logger LOG = LoggerFactory.getLogger(HybridLogicalClock.class);
    public static final long DEFAULT_MAX_DRIFT_MS = 500;

    private final AtomicLong last = new AtomicLong();
    private final LongSupplier physicalMillis;
    private final long maxDriftMs;
    private final LongAdder driftRejections = new LongAdder();

    public HybridLogicalClock(LongSupplier physicalMillis, long maxDriftMs) {
        this.physicalMillis = physicalMillis;
        this.maxDriftMs = maxDriftMs;
    }

    public static HybridLogicalClock system() {
        return new HybridLogicalClock(System::currentTimeMillis, DEFAULT_MAX_DRIFT_MS);
    }

    /** A timestamp for a local event. Strictly greater than every timestamp this clock has issued. */
    public long now() {
        return reserve(1);
    }

    /**
     * Reserves {@code count} consecutive timestamps and returns the first. Used to give every
     * record in a batch its own timestamp with one CAS.
     */
    public long reserve(int count) {
        if (count < 1) {
            throw new IllegalArgumentException("count must be positive");
        }
        while (true) {
            long current = last.get();
            long base = Math.max(current + 1, HybridTime.fromMillis(physicalMillis.getAsLong()));
            if (last.compareAndSet(current, base + count - 1)) {
                return base;
            }
        }
    }

    /**
     * Merges a timestamp received from another node and returns a local timestamp after it.
     *
     * @throws ClockDriftException if {@code remote} is too far ahead of local physical time; the
     *     clock is left unchanged
     */
    public long update(long remote) {
        if (remote < 0) {
            return now();
        }
        long pt = physicalMillis.getAsLong();
        long drift = HybridTime.physicalMillis(remote) - pt;
        if (drift > maxDriftMs) {
            driftRejections.increment();
            LOG.warn("rejected remote timestamp {} ({} ms ahead of local clock, max {} ms)",
                    HybridTime.format(remote), drift, maxDriftMs);
            throw new ClockDriftException("remote timestamp " + drift + " ms ahead", drift);
        }
        long physical = HybridTime.fromMillis(pt);
        while (true) {
            long current = last.get();
            long next = Math.max(Math.max(current, remote) + 1, physical);
            if (last.compareAndSet(current, next)) {
                return next;
            }
        }
    }

    /** Like {@link #update} but never throws; drifted timestamps are counted and ignored. */
    public long observe(long remote) {
        try {
            return update(remote);
        } catch (ClockDriftException e) {
            return now();
        }
    }

    /** The most recently issued timestamp, without advancing the clock. */
    public long current() {
        return last.get();
    }

    public long maxDriftMs() {
        return maxDriftMs;
    }

    public long driftRejections() {
        return driftRejections.sum();
    }
}
