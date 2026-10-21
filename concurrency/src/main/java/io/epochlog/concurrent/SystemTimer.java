package io.epochlog.concurrent;

import io.epochlog.common.Time;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Timer for delayed operations, built on a {@link TimingWheel}. Something must call
 * {@link #advanceClock} in a loop; {@link #startReaper} starts a thread that does.
 *
 * <p>Thread-safety: thread-safe.
 */
public final class SystemTimer implements AutoCloseable {
    private final Executor taskExecutor;
    private final Time time;
    private final DelayQueue<TimerTaskList> delayQueue = new DelayQueue<>();
    private final AtomicInteger taskCounter = new AtomicInteger();
    private final TimingWheel wheel;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile Thread reaper;
    private volatile boolean running = true;

    public SystemTimer(long tickMs, int wheelSize, Time time, Executor taskExecutor) {
        this.time = time;
        this.taskExecutor = taskExecutor;
        this.wheel = new TimingWheel(tickMs, wheelSize, time.monotonicMillis(), taskCounter, delayQueue,
                time::monotonicMillis);
    }

    /** One-millisecond ticks, 20 slots per level, tasks run on the thread that advances the clock. */
    public static SystemTimer create(Time time) {
        return new SystemTimer(1, 20, time, Runnable::run);
    }

    public void add(TimerTask task) {
        lock.readLock().lock();
        try {
            addEntry(new TimerTaskList.Entry(task, time.monotonicMillis() + task.delayMs()));
        } finally {
            lock.readLock().unlock();
        }
    }

    private void addEntry(TimerTaskList.Entry e) {
        if (!wheel.add(e) && !e.cancelled()) {
            taskExecutor.execute(e.task);
        }
    }

    /**
     * Waits up to {@code timeoutMs} for a bucket to expire, then runs everything due.
     *
     * @return true if any bucket was processed
     */
    public boolean advanceClock(long timeoutMs) throws InterruptedException {
        TimerTaskList bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (bucket == null) {
            return false;
        }
        lock.writeLock().lock();
        try {
            while (bucket != null) {
                wheel.advanceClock(bucket.expiration());
                bucket.flush(this::addEntry);
                bucket = delayQueue.poll();
            }
        } finally {
            lock.writeLock().unlock();
        }
        return true;
    }

    public int size() {
        return taskCounter.get();
    }

    public void startReaper(String name) {
        Thread t = Thread.ofPlatform().name(name).daemon().unstarted(() -> {
            while (running) {
                try {
                    advanceClock(200);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        reaper = t;
        t.start();
    }

    @Override
    public void close() {
        running = false;
        Thread t = reaper;
        if (t != null) {
            t.interrupt();
            try {
                t.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
