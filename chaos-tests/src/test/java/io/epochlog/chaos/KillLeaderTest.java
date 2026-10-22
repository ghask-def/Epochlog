package io.epochlog.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import io.epochlog.common.TopicPartition;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Kill the leader while acks=all writes are in flight; no acknowledged record may be lost. */
class KillLeaderTest {
    private static final List<TopicPartition> PARTITIONS = List.of(new TopicPartition("orders", 0),
            new TopicPartition("orders", 1));

    @Test
    void acknowledgedWritesSurviveLeaderCrashes() throws Exception {
        try (ChaosCluster cluster = new ChaosCluster(3).start()) {
            cluster.createTopic("orders", 2, 3);
            History history = new History();
            history.startReading(cluster, PARTITIONS);
            Workload w = new Workload(cluster.producer("acks", "all", "linger.ms", "2"), "orders", "w", history, 300)
                    .start();

            for (int round = 0; round < 3; round++) {
                Thread.sleep(800);
                int victim = cluster.leaderOf(PARTITIONS.get(round % 2));
                cluster.kill(victim);
                ChaosCluster.awaitCondition(() -> cluster.leaderOf(PARTITIONS.get(0)) != victim
                        && cluster.leaderOf(PARTITIONS.get(1)) != victim, 10_000, "re-election");
                Thread.sleep(500);
                cluster.restart(victim);
            }
            Thread.sleep(500);
            w.stop();
            history.stopReading();
            for (TopicPartition tp : PARTITIONS) {
                cluster.awaitConverged(tp, 20_000);
            }
            history.verify(cluster, PARTITIONS);
            assertThat(history.ackedValues()).hasSizeGreaterThan(500);
            assertThat(history.observedCount()).isPositive();
        }
    }
}
