package io.epochlog.replication;

import static org.assertj.core.api.Assertions.assertThat;

import io.epochlog.common.TopicPartition;
import io.epochlog.protocol.MemoryRecords;
import io.epochlog.storage.OffsetAndEpoch;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class EpochTruncationTest {

    /** A follower with a fixed epoch history; offsets are where each epoch starts. */
    record FakeReplica(long leo, long hw, List<long[]> epochs) implements FollowerReplica {
        @Override
        public TopicPartition topicPartition() {
            return new TopicPartition("t", 0);
        }

        @Override
        public int leaderEpoch() {
            return 0;
        }

        @Override
        public long logEndOffset() {
            return leo;
        }

        @Override
        public long highWatermark() {
            return hw;
        }

        @Override
        public int latestLogEpoch() {
            return (int) epochs.get(epochs.size() - 1)[0];
        }

        @Override
        public OffsetAndEpoch endOffsetForEpoch(int epoch) {
            long[] floor = null;
            long[] higher = null;
            for (long[] e : epochs) {
                if (e[0] <= epoch) {
                    floor = e;
                } else if (higher == null) {
                    higher = e;
                }
            }
            if (floor == null) {
                return OffsetAndEpoch.UNDEFINED;
            }
            return new OffsetAndEpoch(higher == null ? leo : higher[1], (int) floor[0]);
        }

        @Override
        public CompletableFuture<Void> appendAsFollower(MemoryRecords records, long hw, int epoch) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> truncateTo(long offset, int epoch) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> truncateFullyAndStartAt(long offset, int epoch) {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void leaderKnowsOurEpochKeepsUpToItsEnd() {
        FakeReplica f = new FakeReplica(120, 90, List.of(new long[] {1, 0}, new long[] {2, 100}));
        EpochTruncation.Decision d = EpochTruncation.decide(new OffsetAndEpoch(110, 2), 2, f);
        assertThat(d).isEqualTo(new EpochTruncation.Decision(110, true));
    }

    @Test
    void zombieTermIsCutOff() {
        // We led epoch 3 briefly from offset 100 while partitioned; the new leader went 2 -> 4.
        FakeReplica f = new FakeReplica(130, 95, List.of(new long[] {2, 0}, new long[] {3, 100}));
        EpochTruncation.Decision d = EpochTruncation.decide(new OffsetAndEpoch(100, 2), 3, f);
        assertThat(d).isEqualTo(new EpochTruncation.Decision(100, true));
    }

    @Test
    void unknownHistoryFallsBackToHighWatermark() {
        FakeReplica f = new FakeReplica(130, 95, List.of(new long[] {2, 0}));
        assertThat(EpochTruncation.decide(OffsetAndEpoch.UNDEFINED, 2, f))
                .isEqualTo(new EpochTruncation.Decision(95, true));
    }

    @Test
    void leaderAnswersWithEpochWeNeverHadSoWeAskAgain() {
        // We have 1 then 3; the leader has 1, 2, 4. It answers "your 3 means my 2, ending at 80".
        FakeReplica f = new FakeReplica(150, 60, List.of(new long[] {1, 0}, new long[] {3, 90}));
        EpochTruncation.Decision d = EpochTruncation.decide(new OffsetAndEpoch(80, 2), 3, f);
        assertThat(d).isEqualTo(new EpochTruncation.Decision(90, false));
    }
}
