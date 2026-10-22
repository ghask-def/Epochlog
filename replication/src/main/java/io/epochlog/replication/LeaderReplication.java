package io.epochlog.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Leader-side replication state for one partition: follower progress, the ISR, and the high
 * watermark rule. The high watermark is the smallest log end offset across the maximal ISR,
 * counting the leader by its own (durable) end offset.
 *
 * <p>Thread-safety: thread-safe. Follower states are independently synchronized; ISR changes
 * swap an immutable {@link IsrState} under this object's monitor.
 */
public final class LeaderReplication {
    private final int localBrokerId;
    private final List<Integer> replicas;
    private final Map<Integer, ReplicaState> followers = new ConcurrentHashMap<>();
    private volatile IsrState isrState;

    public LeaderReplication(int localBrokerId, List<Integer> replicas, List<Integer> isr, int partitionEpoch,
            long nowMs) {
        this.localBrokerId = localBrokerId;
        this.replicas = List.copyOf(replicas);
        for (int id : replicas) {
            if (id != localBrokerId) {
                followers.put(id, new ReplicaState(id, nowMs));
            }
        }
        this.isrState = new IsrState(isr, null, partitionEpoch);
    }

    public IsrState isrState() {
        return isrState;
    }

    public List<Integer> replicas() {
        return replicas;
    }

    public ReplicaState follower(int brokerId) {
        return followers.get(brokerId);
    }

    public boolean isReplica(int brokerId) {
        return followers.containsKey(brokerId);
    }

    /** Records a follower fetch. Returns false if the broker isn't a replica of this partition. */
    public boolean onFollowerFetch(int brokerId, long fetchOffset, long leaderLogEndOffset, long nowMs) {
        ReplicaState r = followers.get(brokerId);
        if (r == null) {
            return false;
        }
        r.onFetch(fetchOffset, leaderLogEndOffset, nowMs);
        return true;
    }

    /**
     * The high watermark the ISR currently supports. A follower that hasn't fetched since this
     * broker became leader reports -1, which holds the watermark where it is.
     */
    public long computeHighWatermark(long leaderEndOffset) {
        long hw = leaderEndOffset;
        for (int id : isrState.maximalIsr()) {
            if (id == localBrokerId) {
                continue;
            }
            ReplicaState r = followers.get(id);
            hw = Math.min(hw, r == null ? -1 : r.logEndOffset());
        }
        return hw;
    }

    /** Followers in the ISR that haven't caught up within {@code maxLagMs}. */
    public List<Integer> laggingFollowers(long nowMs, long maxLagMs) {
        List<Integer> out = new ArrayList<>();
        for (int id : isrState.isr()) {
            ReplicaState r = followers.get(id);
            if (r != null && nowMs - r.lastCaughtUpTimeMs() > maxLagMs) {
                out.add(id);
            }
        }
        return out;
    }

    /**
     * Whether a follower outside the ISR has caught up enough to rejoin: it has everything up to
     * the high watermark, and at least the start of the current leader epoch, so it can't be
     * carrying data from an older leader that this one doesn't have.
     */
    public boolean canJoinIsr(int brokerId, long highWatermark, long leaderEpochStartOffset) {
        if (isrState.isr().contains(brokerId)) {
            return false;
        }
        ReplicaState r = followers.get(brokerId);
        return r != null && r.logEndOffset() >= highWatermark && r.logEndOffset() >= leaderEpochStartOffset;
    }

    /** Starts an ISR change. Returns false if another change is still waiting for the controller. */
    public synchronized boolean propose(List<Integer> newIsr) {
        IsrState current = isrState;
        if (current.hasPendingChange() || current.isr().equals(newIsr)) {
            return false;
        }
        isrState = new IsrState(current.isr(), newIsr, current.partitionEpoch());
        return true;
    }

    /** The controller accepted a change, or sent us a new ISR directly. */
    public synchronized void commit(List<Integer> isr, int partitionEpoch) {
        if (partitionEpoch < isrState.partitionEpoch()) {
            return;
        }
        isrState = new IsrState(isr, null, partitionEpoch);
    }

    /** The controller rejected the pending change. */
    public synchronized void abortPending() {
        IsrState current = isrState;
        isrState = new IsrState(current.isr(), null, current.partitionEpoch());
    }
}
