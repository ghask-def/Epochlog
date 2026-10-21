package io.epochlog.concurrent;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An operation that waits for some condition (replicas catching up, data arriving) or a
 * timeout. Whoever flips {@code completed} first wins: either {@link #tryComplete} sees the
 * condition and calls {@link #forceComplete}, or the timer fires and does.
 * {@link #onComplete} runs exactly once.
 *
 * <p>Thread-safety: thread-safe. {@link #tryComplete} may be called concurrently from several
 * threads and must tolerate that.
 */
public abstract class DelayedOperation extends TimerTask {
    private final AtomicBoolean completed = new AtomicBoolean();

    protected DelayedOperation(long delayMs) {
        super(delayMs);
    }

    /** Completes the operation if nobody else has. Returns true if this call did it. */
    public final boolean forceComplete() {
        if (completed.compareAndSet(false, true)) {
            cancel();
            onComplete();
            return true;
        }
        return false;
    }

    public final boolean isCompleted() {
        return completed.get();
    }

    /** Checks the condition and calls {@link #forceComplete} if it holds. */
    public abstract boolean tryComplete();

    /** Builds and delivers the result. Called once, by whichever thread completed the op. */
    protected abstract void onComplete();

    /** Called after {@link #onComplete} when completion came from the timeout. */
    protected void onExpiration() {}

    @Override
    public final void run() {
        if (forceComplete()) {
            onExpiration();
        }
    }
}
