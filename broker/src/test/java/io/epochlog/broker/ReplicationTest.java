package io.epochlog.broker;

import static org.assertj.core.api.Assertions.assertThat;

import io.epochlog.client.Producer;
import io.epochlog.client.ProducerRecord;
import io.epochlog.client.RecordMetadata;
import io.epochlog.common.Config;
import io.epochlog.common.RecordBatch;
import io.epochlog.common.TopicPartition;
import io.epochlog.protocol.EpochlogApiException;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.message.PartitionState;
import io.epochlog.storage.FileSlice;
import io.epochlog.storage.Log;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReplicationTest {
    private static final TopicPartition TP = new TopicPartition("replicated", 0);

    @TempDir
    Path dir;

    private StubController controller;
    private final Broker[] brokers = new Broker[4];
    private final int[] ports = new int[4];

    private Broker startBroker(int id, int minIsr) throws Exception {
        Broker b = new Broker(BrokerConfig.from(Config.empty()
                .with("broker.id", id).with("host", "127.0.0.1").with("advertised.host", "127.0.0.1")
                .with("port", ports[id]).with("data.dir", dir.resolve("b" + id).toString())
                .with("controller", "127.0.0.1:" + controller.port())
                .with("heartbeat.interval.ms", 100).with("broker.session.timeout.ms", 2000)
                .with("replica.lag.time.max.ms", 1000).with("replica.fetch.max.wait.ms", 100)
                .with("min.insync.replicas", minIsr)));
        b.start();
        ports[id] = b.port();
        brokers[id] = b;
        return b;
    }

    @BeforeEach
    void setUp() throws Exception {
        controller = new StubController();
    }

    private void startCluster(int minIsr) throws Exception {
        for (int id = 1; id <= 3; id++) {
            startBroker(id, minIsr);
        }
        controller.awaitBrokers(3);
        controller.set(new PartitionState(TP.topic(), 0, 1, 0, 0, List.of(1, 2, 3), List.of(1, 2, 3)));
        controller.push();
    }

    @AfterEach
    void tearDown() {
        for (Broker b : brokers) {
            if (b != null) {
                b.close();
            }
        }
        controller.close();
    }

    private Producer producer() {
        Map<String, String> props = new HashMap<>();
        props.put("bootstrap.servers", "127.0.0.1:" + ports[1]);
        props.put("acks", "all");
        props.put("linger.ms", "2");
        props.put("delivery.timeout.ms", "10000");
        props.put("request.timeout.ms", "5000");
        return new Producer(props);
    }

    private static void await(BooleanSupplier cond, String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(20);
        }
    }

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

    private Partition partition(int broker) {
        return brokers[broker].replicaManager().partition(TP).orElseThrow();
    }

    @Test
    void acksAllReplicatesToEveryReplica() throws Exception {
        startCluster(2);
        List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
        try (Producer p = producer()) {
            for (int i = 0; i < 5_000; i++) {
                futures.add(p.send(ProducerRecord.of(TP.topic(), 0, null, ("v" + i).getBytes())));
            }
            p.flush();
        }
        for (CompletableFuture<RecordMetadata> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        // Acknowledged with acks=all means every ISR member had it before the response.
        assertThat(partition(1).highWatermark()).isEqualTo(5_000);
        await(() -> partition(2).logEndOffset() == 5_000 && partition(3).logEndOffset() == 5_000, "followers");
        await(() -> partition(2).highWatermark() == 5_000, "follower high watermark");

        List<RecordBatch> leader = batches(partition(1).log());
        for (int id = 2; id <= 3; id++) {
            List<RecordBatch> follower = batches(partition(id).log());
            assertThat(follower).hasSameSizeAs(leader);
            for (int i = 0; i < leader.size(); i++) {
                assertThat(follower.get(i).buffer()).isEqualTo(leader.get(i).buffer());
            }
        }
    }

    @Test
    void deadFollowerLeavesIsrAndRejoinsAfterRestart() throws Exception {
        startCluster(2);
        try (Producer p = producer()) {
            for (int i = 0; i < 100; i++) {
                p.send(ProducerRecord.of(TP.topic(), 0, null, ("a" + i).getBytes())).get(5, TimeUnit.SECONDS);
            }
            brokers[3].crash();
            await(() -> controller.get(TP.topic(), 0).isr().equals(List.of(1, 2)), "ISR shrink");
            for (int i = 0; i < 100; i++) {
                p.send(ProducerRecord.of(TP.topic(), 0, null, ("b" + i).getBytes())).get(5, TimeUnit.SECONDS);
            }
            assertThat(partition(1).highWatermark()).isEqualTo(200);

            startBroker(3, 2);
            controller.awaitBrokers(3);
            controller.push();
            await(() -> controller.get(TP.topic(), 0).isr().size() == 3, "ISR expand");
            await(() -> partition(3).logEndOffset() == 200, "follower catch-up");
        }
    }

    @Test
    void minInsyncReplicasIsEnforced() throws Exception {
        startCluster(3);
        try (Producer p = producer()) {
            p.send(ProducerRecord.of(TP.topic(), 0, null, "ok".getBytes())).get(5, TimeUnit.SECONDS);
            brokers[2].crash();
            await(() -> controller.get(TP.topic(), 0).isr().size() == 2, "ISR shrink");
        }
        Map<String, String> props = new HashMap<>();
        props.put("bootstrap.servers", "127.0.0.1:" + ports[1]);
        props.put("acks", "all");
        props.put("retries", "0");
        try (Producer p = new Producer(props)) {
            ExecutionException e = org.junit.jupiter.api.Assertions.assertThrows(ExecutionException.class,
                    () -> p.send(ProducerRecord.of(TP.topic(), 0, null, "no".getBytes())).get(5, TimeUnit.SECONDS));
            assertThat(e.getCause()).isInstanceOf(EpochlogApiException.class);
            assertThat(((EpochlogApiException) e.getCause()).error()).isEqualTo(Errors.NOT_ENOUGH_REPLICAS);
        }
    }

    @Test
    void consumersOnlySeeReplicatedData() throws Exception {
        startCluster(2);
        try (Producer p = producer()) {
            p.send(ProducerRecord.of(TP.topic(), 0, null, "x".getBytes())).get(5, TimeUnit.SECONDS);
        }
        // Stop both followers from fetching: new appends stay above the high watermark.
        brokers[2].crash();
        brokers[3].crash();
        Partition leader = partition(1);
        leader.appendAsLeader(new io.epochlog.common.RecordBatchBuilder().append(null, "y".getBytes()).build())
                .get(5, TimeUnit.SECONDS);
        assertThat(leader.logEndOffset()).isEqualTo(2);
        assertThat(leader.highWatermark()).isEqualTo(1);
        assertThat(leader.readCommitted(1, 1024)).isNull();
    }
}
