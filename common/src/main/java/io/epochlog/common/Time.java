package io.epochlog.common;

/**
 * Source of wall-clock and monotonic time. Production code uses {@link #SYSTEM}; tests swap in a
 * {@link ManualTime}.
 *
 * <p>Thread-safety: implementations must be thread-safe.
 */
public interface Time {
    Time SYSTEM = new Time() {
        @Override
        public long milliseconds() {
            return System.currentTimeMillis();
        }

        @Override
        public long nanoseconds() {
            return System.nanoTime();
        }

        @Override
        public void sleep(long ms) throws InterruptedException {
            Thread.sleep(ms);
        }
    };

    long milliseconds();

    long nanoseconds();

    void sleep(long ms) throws InterruptedException;

    /** Monotonic milliseconds; only meaningful for measuring intervals. */
    default long monotonicMillis() {
        return nanoseconds() / 1_000_000L;
    }
}
