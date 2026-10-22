package io.epochlog.broker;

import io.epochlog.concurrent.DelayedOperation;
import java.util.List;

/**
 * A long-poll fetch waiting for data. Consumers wait for the high watermark to move past their
 * fetch offset; followers wait for the log end offset. On completion the read is redone from
 * scratch, so this only tracks whether anything new is available.
 *
 * <p>Thread-safety: thread-safe.
 */
final class DelayedFetch extends DelayedOperation {

    /**
     * One partition being waited on.
     *
     * <p>Thread-safety: immutable.
     */
    record Watch(Partition partition, long fetchOffset) {}

    private final List<Watch> watches;
    private final boolean replica;
    private final Runnable complete;

    DelayedFetch(long maxWaitMs, List<Watch> watches, boolean replica, Runnable complete) {
        super(maxWaitMs);
        this.watches = watches;
        this.replica = replica;
        this.complete = complete;
    }

    @Override
    public boolean tryComplete() {
        for (Watch w : watches) {
            Partition p = w.partition();
            if (!p.isLeader()) {
                return forceComplete();
            }
            long bound = replica ? p.logEndOffset() : p.highWatermark();
            if (bound > w.fetchOffset() || bound < w.fetchOffset()) {
                return forceComplete();
            }
        }
        return false;
    }

    @Override
    protected void onComplete() {
        complete.run();
    }
}
