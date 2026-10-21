package io.epochlog.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link Time} that only moves when told to. {@link #sleep} advances the clock instead of
 * blocking.
 *
 * <p>Thread-safety: thread-safe.
 */
public final class ManualTime implements Time {
    private final AtomicLong wallMillis;
    private final AtomicLong nanos;

    public ManualTime() {
        this(1_700_000_000_000L);
    }

    public ManualTime(long startMillis) {
        this.wallMillis = new AtomicLong(startMillis);
        this.nanos = new AtomicLong(TimeUnit.MILLISECONDS.toNanos(startMillis));
    }

    @Override
    public long milliseconds() {
        return wallMillis.get();
    }

    @Override
    public long nanoseconds() {
        return nanos.get();
    }

    @Override
    public void sleep(long ms) {
        advance(ms);
    }

    public void advance(long ms) {
        wallMillis.addAndGet(ms);
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(ms));
    }

    /** Moves only the wall clock, leaving monotonic time alone. Useful for simulating NTP steps. */
    public void setWallClock(long millis) {
        wallMillis.set(millis);
    }
}
