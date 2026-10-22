package io.epochlog.chaos;

import static org.assertj.core.api.Assertions.assertThat;

import io.epochlog.broker.Partition;
import io.epochlog.client.Producer;
import io.epochlog.client.ProducerRecord;
import io.epochlog.common.TopicPartition;
import io.epochlog.protocol.EpochlogApiException;
import io.epochlog.protocol.Errors;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Isolate the leader from the controller and the other brokers while one producer can still
 * reach it (and only it). The controller elects a new leader with a higher epoch. The old leader
 * must not acknowledge anything; once its lease runs out it rejects writes outright, and when the
 * partition heals it steps down to the new epoch and discards whatever it wrote alone.
 */
class ZombieLeaderTest {
    private static final TopicPartition TP = new TopicPartition("payments", 0);

    @Test
    void zombieLeaderIsFenced() throws Exception {
        try (ChaosCluster cluster = new ChaosCluster(3).start()) {
            cluster.createTopic("payments", 1, 3);
            History history = new History();
            history.startReading(cluster, List.of(TP));
            Workload w = new Workload(cluster.producer("acks", "all"), "payments", "ok", history, 1_000).start();
            Thread.sleep(500);

            int zombie = cluster.leaderOf(TP);
            int oldEpoch = cluster.state(TP).leaderEpoch();
            Partition zombiePartition = cluster.partition(zombie, TP).orElseThrow();

            String stubborn = "stubborn";
            for (int id = 1; id <= 3; id++) {
                if (id != zombie) {
                    cluster.network().partition(stubborn, ChaosCluster.broker(id));
                }
            }
            Map<Errors, Integer> rejections = new ConcurrentHashMap<>();
            List<String> zombieAcked = new ArrayList<>();
            try (Producer p = cluster.producerNamed(stubborn, "acks", "all", "retries", "0",
                    "enable.idempotence", "false", "request.timeout.ms", "800")) {
                // Warm up metadata and the connection before the zombie is cut off.
                p.send(ProducerRecord.of(TP.topic(), 0, null, "warmup".getBytes())).get(5, TimeUnit.SECONDS);
                history.acked("warmup");
                cluster.isolate(zombie);

                List<CompletableFuture<?>> sends = new ArrayList<>();
                for (int i = 0; i < 30; i++) {
                    String v = "zombie-" + i;
                    sends.add(p.send(ProducerRecord.of(TP.topic(), 0, null, v.getBytes())).whenComplete((md, err) -> {
                        if (err == null) {
                            synchronized (zombieAcked) {
                                zombieAcked.add(v);
                            }
                        } else if (err instanceof EpochlogApiException api) {
                            rejections.merge(api.error(), 1, Integer::sum);
                        }
                    }));
                    Thread.sleep(100);
                }
                for (CompletableFuture<?> f : sends) {
                    f.handle((v, e) -> null).get(10, TimeUnit.SECONDS);
                }
            }

            ChaosCluster.awaitCondition(() -> cluster.leaderOf(TP) != zombie, 10_000, "new leader elected");
            assertThat(cluster.state(TP).leaderEpoch()).isGreaterThan(oldEpoch);
            assertThat(zombieAcked).as("writes acknowledged by the isolated leader").isEmpty();
            assertThat(rejections.getOrDefault(Errors.NOT_LEADER_OR_FOLLOWER, 0))
                    .as("writes refused once the lease expired").isPositive();

            cluster.healAll();
            ChaosCluster.awaitCondition(() -> !zombiePartition.isLeader()
                    && zombiePartition.leaderEpoch() > oldEpoch, 15_000, "zombie steps down");
            w.stop();
            history.stopReading();
            cluster.awaitConverged(TP, 20_000);
            var finalLog = history.verify(cluster, List.of(TP));

            // The zombie appended some of those writes locally before its lease ran out; they were
            // truncated away when it rejoined under the new epoch.
            long zombieOnly = finalLog.get(TP).values().stream().filter(v -> v.startsWith("zombie-")).count();
            assertThat(zombieOnly).isZero();
        }
    }
}
