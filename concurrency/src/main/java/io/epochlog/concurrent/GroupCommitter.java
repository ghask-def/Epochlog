package io.epochlog.concurrent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batches fsyncs. Writers append to the page cache and then call {@link #commit}; a dedicated
 * thread collects requests for up to {@code maxWait} (or until {@code maxBatch} requests are
 * waiting), syncs each distinct target once, and completes every future whose offset is covered.
 * Under load one fsync covers many appends, which is where most of the throughput comes from.
 *
 * <p>Thread-safety: {@link #commit} is thread-safe. Syncs run only on the committer thread.
 */
public final class GroupCommitter implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(GroupCommitter.class);

    /**
     * Something that can be made durable.
     *
     * <p>Thread-safety: {@link #sync} is called from the committer thread only, but concurrently
     * with the target's own writer.
     */
    @FunctionalInterface
    public interface Syncable {
        /**
         * Flushes all data written so far.
         *
         * @return offset below which everything is durable
         */
        long sync() throws IOException;
    }

    private record Request(Syncable target, long offset, CompletableFuture<Void> future) {}

    private final MpscArrayQueue<Request> queue;
    private final long maxWaitNanos;
    private final int maxBatch;
    private final Thread thread;
    private final LongAdder syncs = new LongAdder();
    private final LongAdder commits = new LongAdder();
    private volatile boolean parked;
    private volatile boolean running = true;

    public GroupCommitter(String name, long maxWait, TimeUnit unit, int maxBatch) {
        this.queue = new MpscArrayQueue<>(Math.max(1024, maxBatch * 4));
        this.maxWaitNanos = unit.toNanos(maxWait);
        this.maxBatch = maxBatch;
        this.thread = Thread.ofPlatform().name(name).daemon().unstarted(this::loop);
        thread.start();
    }

    /** Completes once everything in {@code target} below {@code offset} is on disk. */
    public CompletableFuture<Void> commit(Syncable target, long offset) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        Request r = new Request(target, offset, f);
        while (!queue.offer(r)) {
            if (!running) {
                f.completeExceptionally(new IllegalStateException("group committer closed"));
                return f;
            }
            LockSupport.unpark(thread);
            Thread.onSpinWait();
        }
        if (parked) {
            LockSupport.unpark(thread);
        }
        return f;
    }

    private void loop() {
        List<Request> pending = new ArrayList<>();
        while (running || !queue.isEmpty()) {
            queue.drain(pending::add, maxBatch);
            if (pending.isEmpty()) {
                park(TimeUnit.MILLISECONDS.toNanos(100));
                continue;
            }
            long deadline = System.nanoTime() + maxWaitNanos;
            while (pending.size() < maxBatch && running) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    break;
                }
                if (queue.drain(pending::add, maxBatch - pending.size()) == 0) {
                    park(remaining);
                }
            }
            syncAll(pending);
            pending.clear();
        }
    }

    private void park(long nanos) {
        parked = true;
        if (queue.isEmpty()) {
            LockSupport.parkNanos(this, nanos);
        }
        parked = false;
    }

    private void syncAll(List<Request> batch) {
        Map<Syncable, Long> durable = new IdentityHashMap<>();
        Map<Syncable, Throwable> failed = new IdentityHashMap<>();
        for (Request r : batch) {
            if (durable.containsKey(r.target) || failed.containsKey(r.target)) {
                continue;
            }
            try {
                durable.put(r.target, r.target.sync());
                syncs.increment();
            } catch (IOException | RuntimeException e) {
                LOG.error("fsync failed", e);
                failed.put(r.target, e);
            }
        }
        commits.add(batch.size());
        for (Request r : batch) {
            Throwable err = failed.get(r.target);
            if (err != null) {
                r.future.completeExceptionally(err);
            } else if (durable.get(r.target) >= r.offset) {
                r.future.complete(null);
            } else {
                // Truncated under us; the offset will never become durable in this log.
                r.future.completeExceptionally(new IllegalStateException(
                        "offset " + r.offset + " not durable after sync (durable up to " + durable.get(r.target) + ")"));
            }
        }
    }

    public long syncCount() {
        return syncs.sum();
    }

    public long commitCount() {
        return commits.sum();
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
