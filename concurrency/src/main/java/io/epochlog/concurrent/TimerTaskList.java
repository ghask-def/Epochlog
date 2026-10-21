package io.epochlog.concurrent;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * One timing-wheel bucket: a circular doubly linked list of entries that share an expiration
 * tick. Buckets go into a {@code DelayQueue} so the timer only wakes up for ticks that have work,
 * rather than every tick.
 *
 * <p>Thread-safety: thread-safe. Structural changes lock the list and then the entry.
 */
final class TimerTaskList implements Delayed {
    private final AtomicInteger taskCounter;
    private final LongSupplier clockMs;
    private final Entry root = new Entry(null, -1);
    private final AtomicLong expiration = new AtomicLong(-1);

    TimerTaskList(AtomicInteger taskCounter, LongSupplier clockMs) {
        this.taskCounter = taskCounter;
        this.clockMs = clockMs;
        root.next = root;
        root.prev = root;
    }

    /** Returns true if the expiration changed, meaning the bucket must be (re)queued. */
    boolean setExpiration(long expirationMs) {
        return expiration.getAndSet(expirationMs) != expirationMs;
    }

    long expiration() {
        return expiration.get();
    }

    void add(Entry e) {
        boolean done = false;
        while (!done) {
            // Detach from any other list first, outside our lock, to keep lock ordering list -> entry.
            e.remove();
            synchronized (this) {
                synchronized (e) {
                    if (e.list == null) {
                        Entry tail = root.prev;
                        e.next = root;
                        e.prev = tail;
                        e.list = this;
                        tail.next = e;
                        root.prev = e;
                        taskCounter.incrementAndGet();
                        done = true;
                    }
                }
            }
        }
    }

    synchronized void remove(Entry e) {
        synchronized (e) {
            if (e.list == this) {
                e.next.prev = e.prev;
                e.prev.next = e.next;
                e.next = null;
                e.prev = null;
                e.list = null;
                taskCounter.decrementAndGet();
            }
        }
    }

    /** Removes every entry and hands it to {@code f}, then resets the expiration. */
    synchronized void flush(Consumer<Entry> f) {
        Entry head = root.next;
        while (head != root) {
            remove(head);
            f.accept(head);
            head = root.next;
        }
        expiration.set(-1);
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(Math.max(expiration.get() - clockMs.getAsLong(), 0), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(expiration.get(), ((TimerTaskList) o).expiration.get());
    }

    /**
     * A task's slot in a bucket.
     *
     * <p>Thread-safety: guarded by its own monitor and its list's monitor.
     */
    static final class Entry {
        final TimerTask task;
        final long expirationMs;
        volatile TimerTaskList list;
        Entry next;
        Entry prev;

        Entry(TimerTask task, long expirationMs) {
            this.task = task;
            this.expirationMs = expirationMs;
            if (task != null) {
                task.setEntry(this);
            }
        }

        boolean cancelled() {
            return task.entry() != this;
        }

        void remove() {
            TimerTaskList current = list;
            // The entry may move to another list concurrently, so retry until it is detached.
            while (current != null) {
                current.remove(this);
                current = list;
            }
        }
    }
}
