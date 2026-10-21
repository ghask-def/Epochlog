package io.epochlog.concurrent;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One dedicated thread fed by an {@link MpscArrayQueue}. Every task runs on that thread, so
 * whatever the tasks touch has a single writer and needs no locks. After each drained batch it
 * runs an optional hook, which the broker uses to publish the high watermark once per batch
 * instead of once per append.
 *
 * <p>The consumer parks when idle. To avoid lost wake-ups it sets the volatile {@code parked}
 * flag and then re-checks the queue; producers enqueue (a CAS on the producer index) and then
 * read {@code parked}. Both sides use synchronization actions, which the JMM totally orders, so
 * either the consumer sees the new element or the producer sees the flag and unparks it.
 *
 * <p>Thread-safety: {@link #execute} is thread-safe. Tasks run on the worker thread only.
 */
public final class SingleWriterExecutor implements Executor, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(SingleWriterExecutor.class);
    private static final int DRAIN_LIMIT = 256;

    private final MpscArrayQueue<Runnable> queue;
    private final Runnable afterBatch;
    private final Thread thread;
    private volatile boolean parked;
    private volatile boolean running = true;

    public SingleWriterExecutor(String name, int capacity, Runnable afterBatch) {
        this.queue = new MpscArrayQueue<>(capacity);
        this.afterBatch = afterBatch;
        this.thread = Thread.ofPlatform().name(name).daemon().unstarted(this::loop);
        thread.start();
    }

    public SingleWriterExecutor(String name) {
        this(name, 4096, null);
    }

    /** Queues a task, spinning if the queue is momentarily full. */
    @Override
    public void execute(Runnable task) {
        int spins = 0;
        while (!queue.offer(task)) {
            if (!running) {
                throw new RejectedExecutionException("executor " + thread.getName() + " is shut down");
            }
            if (++spins > 100) {
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(50));
            } else {
                Thread.onSpinWait();
            }
            wake();
        }
        if (!running) {
            throw new RejectedExecutionException("executor " + thread.getName() + " is shut down");
        }
        wake();
    }

    private void wake() {
        if (parked) {
            LockSupport.unpark(thread);
        }
    }

    public boolean inWorkerThread() {
        return Thread.currentThread() == thread;
    }

    private void loop() {
        while (running || !queue.isEmpty()) {
            int n = queue.drain(this::runSafely, DRAIN_LIMIT);
            if (n > 0) {
                if (afterBatch != null) {
                    runSafely(afterBatch);
                }
                continue;
            }
            parked = true;
            if (queue.isEmpty() && running) {
                LockSupport.parkNanos(this, TimeUnit.MILLISECONDS.toNanos(100));
            }
            parked = false;
        }
    }

    private void runSafely(Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            LOG.error("task failed on {}", thread.getName(), t);
        }
    }

    public int queued() {
        return queue.size();
    }

    @Override
    public void close() {
        running = false;
        LockSupport.unpark(thread);
        try {
            thread.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
