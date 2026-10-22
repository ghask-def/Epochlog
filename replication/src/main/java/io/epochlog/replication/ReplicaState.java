package io.epochlog.replication;

/**
 * A follower's progress as seen by the leader.
 *
 * <p>A follower counts as caught up at time {@code t} if at {@code t} it had fetched everything
 * the leader had. Fetches carry the follower's log end offset, so when a fetch asks for offset
 * {@code >=} the leader's log end at the previous fetch, the follower was caught up as of that
 * previous fetch. The ISR shrinks when this time falls more than {@code replica.lag.time.max.ms}
 * behind.
 *
 * <p>Thread-safety: thread-safe. Updates are synchronized; readers see volatile fields.
 */
public final class ReplicaState {
    private final int brokerId;
    private volatile long logEndOffset = -1;
    private volatile long lastCaughtUpTimeMs;
    private volatile long lastFetchTimeMs;
    private volatile long lastFetchLeaderLogEndOffset = -1;

    public ReplicaState(int brokerId, long nowMs) {
        this.brokerId = brokerId;
        // A follower gets the benefit of the doubt when leadership changes; it has one lag
        // window to show up before it can be dropped.
        this.lastCaughtUpTimeMs = nowMs;
    }

    public int brokerId() {
        return brokerId;
    }

    /** The follower's log end offset, or -1 before its first fetch to this leader. */
    public long logEndOffset() {
        return logEndOffset;
    }

    public long lastCaughtUpTimeMs() {
        return lastCaughtUpTimeMs;
    }

    public synchronized void onFetch(long fetchOffset, long leaderLogEndOffset, long nowMs) {
        if (fetchOffset >= leaderLogEndOffset) {
            lastCaughtUpTimeMs = Math.max(lastCaughtUpTimeMs, nowMs);
        } else if (lastFetchLeaderLogEndOffset >= 0 && fetchOffset >= lastFetchLeaderLogEndOffset) {
            lastCaughtUpTimeMs = Math.max(lastCaughtUpTimeMs, lastFetchTimeMs);
        }
        logEndOffset = fetchOffset;
        lastFetchLeaderLogEndOffset = leaderLogEndOffset;
        lastFetchTimeMs = nowMs;
    }

    @Override
    public String toString() {
        return "Replica(" + brokerId + ", leo=" + logEndOffset + ", caughtUp=" + lastCaughtUpTimeMs + ")";
    }
}
