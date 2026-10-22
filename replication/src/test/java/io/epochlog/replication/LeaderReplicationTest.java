package io.epochlog.replication;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class LeaderReplicationTest {

    @Test
    void highWatermarkIsMinimumOverIsr() {
        LeaderReplication lr = new LeaderReplication(1, List.of(1, 2, 3), List.of(1, 2, 3), 0, 0);
        assertThat(lr.computeHighWatermark(100)).isEqualTo(-1);
        lr.onFollowerFetch(2, 80, 100, 10);
        lr.onFollowerFetch(3, 60, 100, 10);
        assertThat(lr.computeHighWatermark(100)).isEqualTo(60);
        lr.onFollowerFetch(3, 100, 100, 20);
        assertThat(lr.computeHighWatermark(100)).isEqualTo(80);
        assertThat(lr.computeHighWatermark(70)).isEqualTo(70);
    }

    @Test
    void laggingFollowerIsDetectedAndRejoinsWhenCaughtUp() {
        LeaderReplication lr = new LeaderReplication(1, List.of(1, 2, 3), List.of(1, 2, 3), 0, 0);
        lr.onFollowerFetch(2, 100, 100, 1_000);
        lr.onFollowerFetch(3, 10, 100, 1_000);
        assertThat(lr.laggingFollowers(5_000, 10_000)).isEmpty();
        assertThat(lr.laggingFollowers(11_000, 10_000)).containsExactly(3);

        assertThat(lr.propose(List.of(1, 2))).isTrue();
        // While the shrink is pending the old, larger ISR still bounds the watermark.
        assertThat(lr.computeHighWatermark(100)).isEqualTo(10);
        assertThat(lr.propose(List.of(1))).isFalse();
        lr.commit(List.of(1, 2), 1);
        assertThat(lr.computeHighWatermark(100)).isEqualTo(100);

        assertThat(lr.canJoinIsr(3, 100, 0)).isFalse();
        lr.onFollowerFetch(3, 100, 100, 12_000);
        assertThat(lr.canJoinIsr(3, 100, 0)).isTrue();
        assertThat(lr.canJoinIsr(3, 100, 150)).isFalse();
    }

    @Test
    void followerCountsAsCaughtUpAtItsPreviousFetch() {
        ReplicaState r = new ReplicaState(2, 0);
        r.onFetch(0, 50, 100);
        assertThat(r.lastCaughtUpTimeMs()).isEqualTo(0);
        // Asked for 50, which was the leader's end at the previous fetch: caught up as of t=100.
        r.onFetch(50, 80, 200);
        assertThat(r.lastCaughtUpTimeMs()).isEqualTo(100);
        r.onFetch(80, 80, 300);
        assertThat(r.lastCaughtUpTimeMs()).isEqualTo(300);
    }

    @Test
    void staleCommitIsIgnored() {
        LeaderReplication lr = new LeaderReplication(1, List.of(1, 2), List.of(1, 2), 5, 0);
        lr.commit(List.of(1), 4);
        assertThat(lr.isrState().isr()).containsExactly(1, 2);
        lr.propose(List.of(1));
        lr.abortPending();
        assertThat(lr.isrState().hasPendingChange()).isFalse();
    }

    @Test
    void highWatermarkOnlyAdvances() {
        HighWatermark hw = new HighWatermark(10);
        assertThat(hw.advanceTo(5)).isFalse();
        assertThat(hw.advanceTo(12)).isTrue();
        assertThat(hw.get()).isEqualTo(12);
    }
}
