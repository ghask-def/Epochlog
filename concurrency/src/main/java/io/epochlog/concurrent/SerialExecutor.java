package io.epochlog.concurrent;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs tasks one at a time, in submission order, on a delegate executor. The broker gives each
 * connection one of these so requests from a connection are dispatched in order even though
 * the delegate (virtual threads or a pool) is shared.
 *
 * <p>Thread-safety: thread-safe. Tasks never run concurrently with each other, and each task
 * happens-before the next (the {@code running} flag is released and re-acquired between them).
 */
public final class SerialExecutor implements Executor {
    private static final Logger LOG = LoggerFactory.getLogger(SerialExecutor.class);

    private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private final Executor delegate;

    public SerialExecutor(Executor delegate) {
        this.delegate = delegate;
    }

    @Override
    public void execute(Runnable task) {
        tasks.add(task);
        if (running.compareAndSet(false, true)) {
            delegate.execute(this::drain);
        }
    }

    private void drain() {
        while (true) {
            Runnable r;
            while ((r = tasks.poll()) != null) {
                try {
                    r.run();
                } catch (Throwable t) {
                    LOG.error("serial task failed", t);
                }
            }
            running.set(false);
            if (tasks.isEmpty() || !running.compareAndSet(false, true)) {
                return;
            }
        }
    }
}
