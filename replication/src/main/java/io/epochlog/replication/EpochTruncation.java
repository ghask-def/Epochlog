package io.epochlog.replication;

import io.epochlog.storage.OffsetAndEpoch;

/**
 * Works out where a follower's log diverges from a new leader's, given the leader's answer to
 * "where does my latest epoch end in your log?".
 *
 * <p>Three cases. The leader knows our epoch: keep up to its end offset. The leader stops at an
 * earlier epoch (we have a term it never saw): cut back to where that earlier epoch ends in our
 * own log, then ask again about it. The leader has no history: fall back to our high watermark,
 * which is always safe.
 *
 * <p>Thread-safety: thread-safe (stateless).
 */
public final class EpochTruncation {

    /**
     * A truncation point and whether another round is needed.
     *
     * <p>Thread-safety: immutable.
     */
    public record Decision(long truncateTo, boolean done) {}

    private EpochTruncation() {}

    public static Decision decide(OffsetAndEpoch leaderAnswer, int requestedEpoch, FollowerReplica local) {
        long leo = local.logEndOffset();
        if (leaderAnswer.offset() == OffsetAndEpoch.UNDEFINED_OFFSET) {
            return new Decision(Math.min(local.highWatermark(), leo), true);
        }
        if (leaderAnswer.epoch() == requestedEpoch) {
            return new Decision(Math.min(leaderAnswer.offset(), leo), true);
        }
        OffsetAndEpoch ours = local.endOffsetForEpoch(leaderAnswer.epoch());
        if (ours.offset() == OffsetAndEpoch.UNDEFINED_OFFSET) {
            return new Decision(Math.min(local.highWatermark(), leo), true);
        }
        if (ours.epoch() != leaderAnswer.epoch()) {
            // We never had the leader's epoch either; back up to where our matching history ends
            // and ask again.
            return new Decision(Math.min(ours.offset(), leo), false);
        }
        return new Decision(Math.min(Math.min(leaderAnswer.offset(), ours.offset()), leo), true);
    }
}
