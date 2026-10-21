package io.epochlog.concurrent;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/**
 * Hierarchical timing wheel, after Varghese and Lauck and Kafka's implementation. A wheel has
 * {@code wheelSize} buckets each spanning {@code tickMs}. Tasks too far out go to an overflow
 * wheel whose tick is this wheel's whole interval; as time advances they cascade down into finer
 * wheels. Insert and cancel are O(1).
 *
 * <p>Thread-safety: not thread-safe on its own. {@link SystemTimer} guards it with a
 * read-write lock: adds take the read lock (buckets are internally synchronized) and clock
 * advances take the write lock.
 */
final class TimingWheel {
    private final long tickMs;
    private final int wheelSize;
    private final long interval;
    private final AtomicInteger taskCounter;
    private final DelayQueue<TimerTaskList> queue;
    private final LongSupplier clockMs;
    private final TimerTaskList[] buckets;
    private long currentTime;
    private volatile TimingWheel overflowWheel;

    TimingWheel(long tickMs, int wheelSize, long startMs, AtomicInteger taskCounter,
            DelayQueue<TimerTaskList> queue, LongSupplier clockMs) {
        this.tickMs = tickMs;
        this.wheelSize = wheelSize;
        this.interval = tickMs * wheelSize;
        this.taskCounter = taskCounter;
        this.queue = queue;
        this.clockMs = clockMs;
        this.buckets = new TimerTaskList[wheelSize];
        for (int i = 0; i < wheelSize; i++) {
            buckets[i] = new TimerTaskList(taskCounter, clockMs);
        }
        this.currentTime = startMs - (startMs % tickMs);
    }

    private TimingWheel overflow() {
        TimingWheel w = overflowWheel;
        if (w == null) {
            synchronized (this) {
                w = overflowWheel;
                if (w == null) {
                    w = new TimingWheel(interval, wheelSize, currentTime, taskCounter, queue, clockMs);
                    overflowWheel = w;
                }
            }
        }
        return w;
    }

    /** Returns false if the entry is already expired or cancelled and should run (or be dropped) now. */
    boolean add(TimerTaskList.Entry e) {
        long expiration = e.expirationMs;
        if (e.cancelled()) {
            return false;
        }
        if (expiration < currentTime + tickMs) {
            return false;
        }
        if (expiration < currentTime + interval) {
            long virtualId = expiration / tickMs;
            TimerTaskList bucket = buckets[(int) (virtualId % wheelSize)];
            bucket.add(e);
            if (bucket.setExpiration(virtualId * tickMs)) {
                queue.offer(bucket);
            }
            return true;
        }
        return overflow().add(e);
    }

    void advanceClock(long timeMs) {
        if (timeMs >= currentTime + tickMs) {
            currentTime = timeMs - (timeMs % tickMs);
            TimingWheel w = overflowWheel;
            if (w != null) {
                w.advanceClock(currentTime);
            }
        }
    }
}
