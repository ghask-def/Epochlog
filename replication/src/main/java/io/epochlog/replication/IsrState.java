package io.epochlog.replication;

import java.util.ArrayList;
import java.util.List;

/**
 * The in-sync replica set the controller has accepted, plus a change the leader has proposed but
 * not yet had acknowledged. While a change is pending the leader computes the high watermark
 * over the union of both sets ("maximal ISR"): for a shrink that is the old, larger set, and for
 * an expansion it already includes the caught-up newcomer. Either way the high watermark never
 * gets ahead of what the controller could elect from.
 *
 * <p>Thread-safety: immutable.
 */
public record IsrState(List<Integer> isr, List<Integer> pending, int partitionEpoch) {

    public IsrState {
        isr = List.copyOf(isr);
        pending = pending == null ? null : List.copyOf(pending);
    }

    public boolean hasPendingChange() {
        return pending != null;
    }

    public List<Integer> maximalIsr() {
        if (pending == null) {
            return isr;
        }
        List<Integer> union = new ArrayList<>(isr);
        for (int id : pending) {
            if (!union.contains(id)) {
                union.add(id);
            }
        }
        return union;
    }
}
