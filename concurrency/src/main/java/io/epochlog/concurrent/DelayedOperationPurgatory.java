package io.epochlog.concurrent;

import io.epochlog.common.Time;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Holds delayed operations keyed by what they wait on (usually a partition) until they
 * complete or time out. Code that changes state, like advancing a high watermark, calls
 * {@link #checkAndComplete} for the affected key.
 *
 * <p>Thread-safety: thread-safe.
 */
public final class DelayedOperationPurgatory<T extends DelayedOperation> implements AutoCloseable {
    private final ConcurrentHashMap<Object, Queue<T>> watchers = new ConcurrentHashMap<>();
    private final SystemTimer timer;
    private final ExecutorService expirationExecutor;
    private final int purgeInterval;
    private final AtomicInteger sinceLastPurge = new AtomicInteger();

    public DelayedOperationPurgatory(String name, Time time, int purgeInterval) {
        // Expired operations build responses, so run them off the reaper thread to keep the
        // timer's write lock short.
        this.expirationExecutor = Executors.newSingleThreadExecutor(
                Thread.ofPlatform().name(name + "-expiration").daemon().factory());
        this.timer = new SystemTimer(1, 20, time, expirationExecutor);
        this.purgeInterval = purgeInterval;
        timer.startReaper(name + "-reaper");
    }

    /**
     * Completes {@code op} now if possible; otherwise watches it under each key and arms its
     * timeout. Returns true if it completed immediately.
     */
    public boolean tryCompleteElseWatch(T op, List<?> keys) {
        if (op.tryComplete()) {
            return true;
        }
        for (Object key : keys) {
            if (op.isCompleted()) {
                return false;
            }
            watchers.computeIfAbsent(key, k -> new ConcurrentLinkedQueue<>()).add(op);
        }
        // Re-check after registering: the condition may have changed between the first check and
        // the add, and the thread that changed it may have scanned the watchers before we got in.
        if (op.tryComplete()) {
            return true;
        }
        if (!op.isCompleted()) {
            timer.add(op);
            if (op.isCompleted()) {
                op.cancel();
            }
        }
        if (sinceLastPurge.incrementAndGet() >= purgeInterval) {
            sinceLastPurge.set(0);
            purgeCompleted();
        }
        return false;
    }

    /** Re-checks every operation watching {@code key}. Returns how many completed. */
    public int checkAndComplete(Object key) {
        Queue<T> ops = watchers.get(key);
        if (ops == null || ops.isEmpty()) {
            return 0;
        }
        int done = 0;
        for (Iterator<T> it = ops.iterator(); it.hasNext(); ) {
            T op = it.next();
            if (op.isCompleted()) {
                it.remove();
            } else if (op.tryComplete()) {
                it.remove();
                done++;
            }
        }
        return done;
    }

    /** Number of operations waiting under {@code key}, including completed ones not yet purged. */
    public int watched(Object key) {
        Queue<T> ops = watchers.get(key);
        return ops == null ? 0 : ops.size();
    }

    /** Operations still armed in the timer. */
    public int delayed() {
        return timer.size();
    }

    private void purgeCompleted() {
        for (Queue<T> ops : watchers.values()) {
            ops.removeIf(DelayedOperation::isCompleted);
        }
    }

    @Override
    public void close() {
        timer.close();
        expirationExecutor.shutdownNow();
    }
}
