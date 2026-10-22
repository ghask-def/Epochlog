package io.epochlog.controller;

import static org.assertj.core.api.Assertions.assertThat;

import io.epochlog.broker.Broker;
import io.epochlog.broker.BrokerConfig;
import io.epochlog.broker.Partition;
import io.epochlog.client.Admin;
import io.epochlog.client.Consumer;
import io.epochlog.client.ConsumerRecord;
import io.epochlog.client.Producer;
import io.epochlog.client.ProducerRecord;
import io.epochlog.common.Config;
import io.epochlog.common.TopicPartition;
import io.epochlog.network.NetworkClient;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.message.LeaderAndIsrRequest;
import io.epochlog.protocol.message.PartitionErrorsResponse;
import io.epochlog.protocol.message.PartitionState;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FailoverTest {
    private static final TopicPartition TP = new TopicPartition("ha", 0);

    @TempDir
    Path dir;

    private Controller controller;
    private final Broker[] brokers = new Broker[4];
    private final int[] ports = new int[4];

    private void startController() throws Exception {
        controller = new Controller(ControllerConfig.from(Config.empty()
                .with("host", "127.0.0.1").with("port", 0).with("data.dir", dir.resolve("controller").toString())
                .with("broker.session.timeout.ms", 1500)));
        controller.start();
    }

    private void startBroker(int id) throws Exception {
        Broker b = new Broker(BrokerConfig.from(Config.empty()
                .with("broker.id", id).with("host", "127.0.0.1").with("advertised.host", "127.0.0.1")
                .with("port", ports[id]).with("data.dir", dir.resolve("b" + id).toString())
                .with("controller", "127.0.0.1:" + controller.port())
                .with("heartbeat.interval.ms", 100).with("broker.session.timeout.ms", 1500)
                .with("replica.lag.time.max.ms", 1000).with("replica.fetch.max.wait.ms", 100)
                .with("min.insync.replicas", 2)));
        b.start();
        ports[id] = b.port();
        brokers[id] = b;
    }

    @BeforeEach
    void setUp() throws Exception {
        startController();
        for (int id = 1; id <= 3; id++) {
            startBroker(id);
        }
        await(() -> controller.isBrokerAlive(1) && controller.isBrokerAlive(2) && controller.isBrokerAlive(3),
                "brokers registered");
        try (Admin admin = new Admin(bootstrap())) {
            admin.createTopic(TP.topic(), 1, 3, Map.of());
        }
    }

    @AfterEach
    void tearDown() {
        for (Broker b : brokers) {
            if (b != null) {
                b.close();
            }
        }
        if (controller != null) {
            controller.close();
        }
    }

    private String bootstrap() {
        StringBuilder sb = new StringBuilder();
        for (int id = 1; id <= 3; id++) {
            if (brokers[id] != null && brokers[id].isStarted()) {
                sb.append(sb.isEmpty() ? "" : ",").append("127.0.0.1:").append(ports[id]);
            }
        }
        return sb.toString();
    }

    private static void await(BooleanSupplier cond, String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(20);
        }
    }

    private Partition partition(int id) {
        return brokers[id].replicaManager().partition(TP).orElseThrow();
    }

    @Test
    void leaderFailureElectsNewLeaderWithoutLosingAcknowledgedWrites() throws Exception {
        Set<String> acked = new HashSet<>();
        Map<String, String> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrap());
        props.put("acks", "all");
        props.put("linger.ms", "1");
        props.put("request.timeout.ms", "3000");
        props.put("delivery.timeout.ms", "30000");

        PartitionState before = controller.partitionState(TP);
        int oldLeader = before.leader();
        try (Producer p = new Producer(props)) {
            List<CompletableFuture<?>> pending = new ArrayList<>();
            for (int i = 0; i < 2_000; i++) {
                String v = "m" + i;
                pending.add(p.send(ProducerRecord.of(TP.topic(), null, v)).thenRun(() -> {
                    synchronized (acked) {
                        acked.add(v);
                    }
                }));
                if (i == 1_000) {
                    brokers[oldLeader].crash();
                }
                if (i % 100 == 0) {
                    Thread.sleep(10);
                }
            }
            p.flush();
            for (CompletableFuture<?> f : pending) {
                f.handle((v, e) -> null).get(30, TimeUnit.SECONDS);
            }
        }

        PartitionState after = controller.partitionState(TP);
        assertThat(after.leader()).isNotEqualTo(oldLeader);
        assertThat(after.leaderEpoch()).isGreaterThan(before.leaderEpoch());
        assertThat(acked).hasSizeGreaterThan(1_500);

        // Every acknowledged record must be readable from the new leader.
        Set<String> seen = new HashSet<>();
        props.put("bootstrap.servers", bootstrap());
        try (Consumer c = new Consumer(props)) {
            c.assign(List.of(TP));
            long deadline = System.currentTimeMillis() + 20_000;
            while (!seen.containsAll(acked) && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord r : c.poll(Duration.ofMillis(200))) {
                    seen.add(r.valueString());
                }
            }
        }
        assertThat(seen).containsAll(acked);

        // The old leader comes back as a follower, drops anything it had that the new leader
        // doesn't, and catches up.
        startBroker(oldLeader);
        await(() -> controller.partitionState(TP).isr().size() == 3, "old leader rejoins ISR");
        int newLeader = controller.partitionState(TP).leader();
        await(() -> partition(oldLeader).logEndOffset() == partition(newLeader).logEndOffset(), "logs converge");
        assertThat(partition(oldLeader).isLeader()).isFalse();
    }

    @Test
    void brokersRejectStaleLeaderEpochs() throws Exception {
        PartitionState current = controller.partitionState(TP);
        int follower = current.replicas().stream().filter(id -> id != current.leader()).findFirst().orElseThrow();
        await(() -> brokers[follower].replicaManager().partition(TP)
                .map(p -> p.leaderEpoch() == current.leaderEpoch()).orElse(false), "follower up to date");

        // A zombie controller (or a delayed message) tries to install an older epoch.
        PartitionState stale = new PartitionState(TP.topic(), 0, follower, current.leaderEpoch() - 1, 99,
                current.replicas(), current.isr());
        try (NetworkClient client = new NetworkClient("test", "zombie", 5_000)) {
            PartitionErrorsResponse resp = client.send(new InetSocketAddress("127.0.0.1", ports[follower]),
                    new LeaderAndIsrRequest(0, Long.MAX_VALUE, List.of(), List.of(stale), List.of()),
                    PartitionErrorsResponse::read).get(5, TimeUnit.SECONDS);
            assertThat(resp.partitions()).extracting(PartitionErrorsResponse.PartitionError::error)
                    .containsExactly(Errors.FENCED_LEADER_EPOCH);
        }
        assertThat(partition(follower).isLeader()).isFalse();
    }

    @Test
    void controllerRestartKeepsAssignmentsAndEpochs() throws Exception {
        PartitionState before = controller.partitionState(TP);
        controller.close();
        controller = new Controller(ControllerConfig.from(Config.empty()
                .with("host", "127.0.0.1").with("port", 0).with("data.dir", dir.resolve("controller").toString())
                .with("broker.session.timeout.ms", 1500)));
        assertThat(controller.partitionState(TP)).isEqualTo(before);
    }
}
