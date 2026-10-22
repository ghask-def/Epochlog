package io.epochlog.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import io.epochlog.common.RecordBatch;
import io.epochlog.common.TopicPartition;
import io.epochlog.storage.FileSlice;
import io.epochlog.storage.Log;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Cut a follower off from its leader: it leaves the ISR, rejoins once healed, and the logs end up identical. */
class FollowerPartitionTest {
    private static final TopicPartition TP = new TopicPartition("events", 0);

    private static List<RecordBatch> batches(Log log) throws Exception {
        List<RecordBatch> out = new ArrayList<>();
        long offset = log.logStartOffset();
        while (offset < log.logEndOffset()) {
            FileSlice slice = log.read(offset, 1 << 20, Long.MAX_VALUE);
            for (RecordBatch b : slice.batches()) {
                out.add(b);
                offset = b.nextOffset();
            }
        }
        return out;
    }

    @Test
    void partitionedFollowerDropsOutAndRejoins() throws Exception {
        try (ChaosCluster cluster = new ChaosCluster(3).start()) {
            cluster.createTopic("events", 1, 3);
            History history = new History();
            history.startReading(cluster, List.of(TP));
            Workload w = new Workload(cluster.producer("acks", "all"), "events", "f", history, 500).start();
            Thread.sleep(500);

            int leader = cluster.leaderOf(TP);
            int follower = cluster.state(TP).replicas().stream().filter(id -> id != leader).findFirst().orElseThrow();
            cluster.network().partition(ChaosCluster.broker(leader), ChaosCluster.broker(follower));

            ChaosCluster.awaitCondition(() -> !cluster.state(TP).isr().contains(follower), 10_000, "ISR shrink");
            int ackedWhileOut = history.ackedValues().size();
            Thread.sleep(500);
            assertThat(history.ackedValues().size()).as("writes continue without the follower")
                    .isGreaterThan(ackedWhileOut);

            cluster.healAll();
            ChaosCluster.awaitCondition(() -> cluster.state(TP).isr().contains(follower), 15_000, "ISR rejoin");
            w.stop();
            history.stopReading();
            cluster.awaitConverged(TP, 20_000);

            List<RecordBatch> leaderLog = batches(cluster.partition(cluster.leaderOf(TP), TP).orElseThrow().log());
            for (int id : cluster.state(TP).replicas()) {
                List<RecordBatch> replica = batches(cluster.partition(id, TP).orElseThrow().log());
                assertThat(replica).hasSameSizeAs(leaderLog);
                for (int i = 0; i < replica.size(); i++) {
                    assertThat(replica.get(i).buffer()).as("batch %d on broker %d", i, id)
                            .isEqualTo(leaderLog.get(i).buffer());
                }
            }
            history.verify(cluster, List.of(TP));
        }
    }
}
