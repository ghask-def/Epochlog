package io.epochlog.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import io.epochlog.common.TopicPartition;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Keep resetting the producer's connections and slowing its responses past the request timeout,
 * so it retries batches the broker already wrote. With idempotence on, every record must appear
 * exactly once.
 */
class RetryStormTest {
    private static final TopicPartition TP = new TopicPartition("storm", 0);

    @Test
    void idempotentProducerWritesEachRecordOnce() throws Exception {
        try (ChaosCluster cluster = new ChaosCluster(3).start()) {
            cluster.createTopic("storm", 1, 3);
            History history = new History();
            history.startReading(cluster, List.of(TP));
            Workload w = new Workload(cluster.producerNamed("storm-producer", "acks", "all", "linger.ms", "1",
                    "request.timeout.ms", "300", "retry.backoff.ms", "20"), "storm", "s", history, 200).start();

            int leader = cluster.leaderOf(TP);
            String producerLink = "storm-producer";
            for (int i = 0; i < 20; i++) {
                Thread.sleep(150);
                if (i % 3 == 0) {
                    // Responses arrive after the producer gave up on them.
                    cluster.network().delay(ChaosCluster.broker(leader), producerLink, 400);
                    cluster.network().delay(producerLink, ChaosCluster.broker(leader), 0);
                } else {
                    cluster.network().delay(ChaosCluster.broker(leader), producerLink, 0);
                }
                cluster.network().dropConnections(producerLink, ChaosCluster.broker(leader));
            }
            cluster.healAll();
            w.stop();
            history.stopReading();
            cluster.awaitConverged(TP, 20_000);
            Map<TopicPartition, Map<Long, String>> finalLog = history.verify(cluster, List.of(TP));

            Map<String, Integer> counts = new HashMap<>();
            for (String v : finalLog.get(TP).values()) {
                counts.merge(v, 1, Integer::sum);
            }
            List<String> dupes = counts.entrySet().stream().filter(e -> e.getValue() > 1).map(Map.Entry::getKey).toList();
            assertThat(dupes).as("duplicated records").isEmpty();
            assertThat(history.ackedValues()).hasSizeGreaterThan(200);
        }
    }
}
