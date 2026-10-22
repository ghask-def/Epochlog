package io.epochlog.chaos;

import io.epochlog.broker.Broker;
import io.epochlog.broker.BrokerConfig;
import io.epochlog.broker.Partition;
import io.epochlog.client.Admin;
import io.epochlog.client.ClientConfig;
import io.epochlog.client.Consumer;
import io.epochlog.client.Producer;
import io.epochlog.common.Config;
import io.epochlog.common.Time;
import io.epochlog.common.TopicPartition;
import io.epochlog.controller.Controller;
import io.epochlog.controller.ControllerConfig;
import io.epochlog.protocol.message.PartitionState;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

/**
 * A controller and N brokers in one JVM, every link going through a {@link ChaosNetwork}, every
 * broker with its own adjustable wall clock. Timeouts are scaled down so failures are detected in
 * about a second.
 *
 * <p>Thread-safety: not thread-safe; drive it from the test thread.
 */
public final class ChaosCluster implements Closeable {
    public static final String CONTROLLER = "controller";
    public static final long SESSION_TIMEOUT_MS = 1_500;

    private final Path dir;
    private final int size;
    private final ChaosNetwork network = new ChaosNetwork();
    private final Broker[] brokers;
    private final int[] ports;
    private final AtomicLong[] clockSkews;
    private final AtomicInteger clientIds = new AtomicInteger();
    private Controller controller;

    public ChaosCluster(int size) throws IOException {
        this.dir = Files.createTempDirectory("epochlog-chaos");
        this.size = size;
        this.brokers = new Broker[size + 1];
        this.ports = new int[size + 1];
        this.clockSkews = new AtomicLong[size + 1];
        for (int i = 1; i <= size; i++) {
            clockSkews[i] = new AtomicLong();
        }
    }

    public static String broker(int id) {
        return "broker-" + id;
    }

    public ChaosCluster start() throws Exception {
        controller = new Controller(ControllerConfig.from(Config.empty()
                .with("host", "127.0.0.1").with("port", 0).with("data.dir", dir.resolve("controller").toString())
                .with("broker.session.timeout.ms", SESSION_TIMEOUT_MS)), network.resolverFor(CONTROLLER));
        controller.start();
        network.register(CONTROLLER, controller.port());
        for (int id = 1; id <= size; id++) {
            startBroker(id);
        }
        awaitCondition(() -> {
            for (int id = 1; id <= size; id++) {
                if (!controller.isBrokerAlive(id)) {
                    return false;
                }
            }
            return true;
        }, 20_000, "all brokers registered");
        return this;
    }

    public void startBroker(int id) throws Exception {
        AtomicLong skew = clockSkews[id];
        Broker b = new Broker(BrokerConfig.from(Config.empty()
                .with("broker.id", id).with("host", "127.0.0.1").with("advertised.host", "127.0.0.1")
                .with("port", ports[id]).with("data.dir", dir.resolve("broker-" + id).toString())
                .with("controller", "127.0.0.1:" + controller.port())
                .with("heartbeat.interval.ms", 100).with("broker.session.timeout.ms", SESSION_TIMEOUT_MS)
                .with("replica.lag.time.max.ms", 1_000).with("replica.fetch.max.wait.ms", 100)
                .with("request.timeout.ms", 5_000).with("min.insync.replicas", 2)
                .with("offsets.topic.partitions", 2)),
                Time.SYSTEM, () -> System.currentTimeMillis() + skew.get(), network.resolverFor(broker(id)));
        b.start();
        ports[id] = b.port();
        network.register(broker(id), b.port());
        brokers[id] = b;
    }

    public void kill(int id) {
        brokers[id].crash();
    }

    public void restart(int id) throws Exception {
        startBroker(id);
    }

    public Broker brokerInstance(int id) {
        return brokers[id];
    }

    public Controller controller() {
        return controller;
    }

    public ChaosNetwork network() {
        return network;
    }

    public void setClockSkew(int id, long ms) {
        clockSkews[id].set(ms);
    }

    /** Cuts a broker off from the controller and every other broker. Client links stay up. */
    public void isolate(int id) {
        network.partition(broker(id), CONTROLLER);
        for (int other = 1; other <= size; other++) {
            if (other != id) {
                network.partition(broker(id), broker(other));
            }
        }
    }

    public void healAll() {
        network.healAll();
    }

    public String bootstrap() {
        StringBuilder sb = new StringBuilder();
        for (int id = 1; id <= size; id++) {
            sb.append(sb.isEmpty() ? "" : ",").append("127.0.0.1:").append(ports[id]);
        }
        return sb.toString();
    }

    private String nextClientName() {
        return "client-" + clientIds.incrementAndGet();
    }

    public Map<String, String> clientProps(String... kv) {
        Map<String, String> m = new HashMap<>();
        m.put("bootstrap.servers", bootstrap());
        m.put("request.timeout.ms", "3000");
        m.put("delivery.timeout.ms", "60000");
        m.put("retry.backoff.ms", "50");
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    public Producer producer(String... kv) {
        return new Producer(ClientConfig.from(clientProps(kv)), network.resolverFor(nextClientName()));
    }

    /** A producer that talks to brokers under a fixed name, so tests can cut its links. */
    public Producer producerNamed(String name, String... kv) {
        return new Producer(ClientConfig.from(clientProps(kv)), network.resolverFor(name));
    }

    public Consumer consumer(String... kv) {
        return new Consumer(ClientConfig.from(clientProps(kv)), network.resolverFor(nextClientName()));
    }

    public void createTopic(String name, int partitions, int replicationFactor) throws Exception {
        try (Admin admin = new Admin(ClientConfig.from(clientProps()), network.resolverFor(nextClientName()))) {
            admin.createTopic(name, partitions, replicationFactor, Map.of());
        }
    }

    public PartitionState state(TopicPartition tp) {
        return controller.partitionState(tp);
    }

    public int leaderOf(TopicPartition tp) {
        return controller.partitionState(tp).leader();
    }

    public Optional<Partition> partition(int brokerId, TopicPartition tp) {
        Broker b = brokers[brokerId];
        if (b == null || !b.isStarted()) {
            return Optional.empty();
        }
        return b.replicaManager().partition(tp);
    }

    /** Waits until every live replica of {@code tp} has the same log end offset as the leader and all are in the ISR. */
    public void awaitConverged(TopicPartition tp, long timeoutMs) throws InterruptedException {
        awaitCondition(() -> {
            PartitionState ps = controller.partitionState(tp);
            if (ps.leader() < 0 || ps.isr().size() != ps.replicas().size()) {
                return false;
            }
            Optional<Partition> leader = partition(ps.leader(), tp);
            if (leader.isEmpty()) {
                return false;
            }
            long leo = leader.get().logEndOffset();
            for (int id : ps.replicas()) {
                Optional<Partition> p = partition(id, tp);
                if (p.isEmpty() || p.get().logEndOffset() != leo) {
                    return false;
                }
            }
            return leader.get().highWatermark() == leo;
        }, timeoutMs, "replicas of " + tp + " to converge");
    }

    public static void awaitCondition(BooleanSupplier cond, long timeoutMs, String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("timed out after " + timeoutMs + " ms waiting for " + what);
            }
            Thread.sleep(20);
        }
    }

    @Override
    public void close() {
        for (int id = 1; id <= size; id++) {
            if (brokers[id] != null) {
                brokers[id].close();
            }
        }
        if (controller != null) {
            controller.close();
        }
        network.close();
        try (Stream<Path> walk = Files.walk(dir)) {
            for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(p);
            }
        } catch (IOException ignored) {
        }
    }
}
