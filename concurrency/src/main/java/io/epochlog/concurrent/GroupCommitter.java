package io.epochlog.concurrent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
 * When a round covers several files (one per partition), their fsyncs are issued in parallel;
 * the device handles them concurrently, so a round costs about one fsync rather than one per
 * partition.
 *
 * <p>Thread-safety: {@link #commit} is thread-safe. Rounds are driven by the committer thread;
 * within a round each target is synced by at most one thread.
 */
public final class GroupCommitter implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(GroupCommitter.class);

    /**
     * Something that can be made durable.
     *
     * <p>Thread-safety: {@link #sync} is never called concurrently with itself for the same
     * target, but may run concurrently with the target's own writer and with other targets' syncs.
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
    private final ExecutorService syncPool = Executors.newVirtualThreadPerTaskExecutor();
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
        Map<Syncable, Future<Long>> inFlight = new IdentityHashMap<>();
        for (Request r : batch) {
            inFlight.computeIfAbsent(r.target, t -> syncPool.submit(t::sync));
        }
        Map<Syncable, Long> durable = new IdentityHashMap<>();
        Map<Syncable, Throwable> failed = new IdentityHashMap<>();
        for (Map.Entry<Syncable, Future<Long>> e : inFlight.entrySet()) {
            try {
                durable.put(e.getKey(), e.getValue().get());
                syncs.increment();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                failed.put(e.getKey(), ie);
            } catch (ExecutionException ee) {
                LOG.error("fsync failed", ee.getCause());
                failed.put(e.getKey(), ee.getCause());
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
        syncPool.shutdown();
    }
}
