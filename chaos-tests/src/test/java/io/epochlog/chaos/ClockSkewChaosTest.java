package io.epochlog.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import io.epochlog.client.Consumer;
import io.epochlog.client.ConsumerRecord;
import io.epochlog.common.HybridTime;
import io.epochlog.common.TopicPartition;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Brokers' wall clocks disagree by up to 600 ms. Leadership moves between them several times;
 * HLC timestamps in the partition must still increase strictly with offset.
 */
class ClockSkewChaosTest {
    private static final TopicPartition TP = new TopicPartition("ticks", 0);

    @Test
    void timestampsStayMonotonicAcrossSkewedLeaders() throws Exception {
        try (ChaosCluster cluster = new ChaosCluster(3)) {
            cluster.setClockSkew(1, -300);
            cluster.setClockSkew(2, 0);
            cluster.setClockSkew(3, 300);
            cluster.start();
            cluster.createTopic("ticks", 1, 3);
            History history = new History();
            Workload w = new Workload(cluster.producer("acks", "all", "linger.ms", "1"), "ticks", "t", history, 500)
                    .start();
            for (int round = 0; round < 4; round++) {
                Thread.sleep(600);
                int leader = cluster.leaderOf(TP);
                cluster.kill(leader);
                ChaosCluster.awaitCondition(() -> cluster.leaderOf(TP) != leader, 10_000, "failover");
                cluster.restart(leader);
                ChaosCluster.awaitCondition(() -> cluster.state(TP).isr().size() == 3, 15_000, "rejoin");
            }
            w.stop();
            cluster.awaitConverged(TP, 20_000);

            long previous = -1;
            long count = 0;
            try (Consumer c = cluster.consumer("auto.offset.reset", "earliest")) {
                c.assign(List.of(TP));
                c.seekToEnd(List.of(TP));
                long end = c.position(TP);
                c.seekToBeginning(List.of(TP));
                while (c.position(TP) < end) {
                    for (ConsumerRecord r : c.poll(Duration.ofMillis(100))) {
                        assertThat(r.timestamp()).as("timestamp at offset %d (%s)", r.offset(),
                                HybridTime.format(r.timestamp())).isGreaterThan(previous);
                        previous = r.timestamp();
                        count++;
                    }
                }
            }
            assertThat(count).isGreaterThan(200);
            // No assertion on drift rejections: brokers 1 and 3 are 600 ms apart, past the 500 ms
            // limit, so a follower may refuse to merge a leader's timestamps. That only costs
            // causality tracking on the follower; the log itself never lets a timestamp go back.
        }
    }
}
