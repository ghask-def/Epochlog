package io.epochlog.concurrent;

/**
 * A task scheduled on a {@link SystemTimer}. Cancelling unlinks it from its bucket in O(1).
 *
 * <p>Thread-safety: thread-safe. {@link #cancel} may race with expiry; if expiry already pulled
 * the task out of its bucket, it may still run once.
 */
public abstract class TimerTask implements Runnable {
    private final long delayMs;
    private volatile TimerTaskList.Entry entry;

    protected TimerTask(long delayMs) {
        this.delayMs = delayMs;
    }

    public long delayMs() {
        return delayMs;
    }

    public void cancel() {
        synchronized (this) {
            if (entry != null) {
                entry.remove();
            }
            entry = null;
        }
    }

    synchronized void setEntry(TimerTaskList.Entry e) {
        // A task re-added to the timer moves to its new entry.
        if (entry != null && entry != e) {
            entry.remove();
        }
        entry = e;
    }

    // Deliberately unsynchronized: bucket flushes read this while holding the bucket lock, and
    // cancel() takes the task lock before the bucket lock, so locking here could deadlock.
    TimerTaskList.Entry entry() {
        return entry;
    }
}
