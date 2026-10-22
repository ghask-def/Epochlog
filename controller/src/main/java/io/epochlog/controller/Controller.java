package io.epochlog.controller;

import io.epochlog.common.TopicPartition;
import io.epochlog.network.NetworkClient;
import io.epochlog.network.RequestContext;
import io.epochlog.network.SocketServer;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.OffsetsTopic;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.TopicNames;
import io.epochlog.protocol.message.AlterIsrRequest;
import io.epochlog.protocol.message.AlterIsrResponse;
import io.epochlog.protocol.message.BrokerInfo;
import io.epochlog.protocol.message.CreateTopicRequest;
import io.epochlog.protocol.message.ErrorResponse;
import io.epochlog.protocol.message.HeartbeatRequest;
import io.epochlog.protocol.message.HeartbeatResponse;
import io.epochlog.protocol.message.LeaderAndIsrRequest;
import io.epochlog.protocol.message.MetadataRequest;
import io.epochlog.protocol.message.MetadataResponse;
import io.epochlog.protocol.message.PartitionErrorsResponse;
import io.epochlog.protocol.message.PartitionState;
import io.epochlog.protocol.message.TopicConfig;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The single cluster controller. It tracks broker liveness through heartbeats, assigns
 * partitions, elects a new leader from the ISR when one dies (bumping the leader epoch so the
 * old one is fenced), validates ISR changes proposed by leaders, and pushes the resulting
 * metadata to every live broker. High availability for the controller itself is a non-goal; see
 * docs/adr/0005-single-controller.md.
 *
 * <p>Thread-safety: thread-safe. All state changes happen under this object's monitor; pushes to
 * brokers are asynchronous and happen outside it.
 */
public final class Controller implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(Controller.class);
    public static final int CONTROLLER_ID = 0;

    private static final class BrokerEntry {
        final int id;
        String host;
        int port;
        long epoch;
        boolean alive;
        long lastHeartbeatNanos;

        BrokerEntry(int id) {
            this.id = id;
        }

        BrokerInfo info() {
            return new BrokerInfo(id, host, port);
        }
    }

    private final ControllerConfig config;
    private final ControllerState state;
    private final Map<Integer, BrokerEntry> brokers = new TreeMap<>();
    private final NetworkClient client;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService handlers = Executors.newVirtualThreadPerTaskExecutor();
    private final SocketServer server;

    public Controller(ControllerConfig config, UnaryOperator<InetSocketAddress> resolver) throws IOException {
        this.config = config;
        this.state = ControllerState.load(config.dataDir());
        this.client = new NetworkClient("controller", "controller", config.requestTimeoutMs(), resolver);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("controller-failure-detector").daemon().factory());
        this.server = new SocketServer("controller", config.host(), config.port(), 2, 16 << 20, this::handle, handlers);
        long now = System.nanoTime();
        // Brokers named in the saved state get one session timeout to check in before we start
        // electing around them.
        for (PartitionState ps : state.partitions.values()) {
            for (int id : ps.replicas()) {
                BrokerEntry b = brokers.computeIfAbsent(id, BrokerEntry::new);
                b.alive = true;
                b.lastHeartbeatNanos = now;
                b.epoch = -1;
            }
        }
    }

    public Controller(ControllerConfig config) throws IOException {
        this(config, UnaryOperator.identity());
    }

    public void start() throws IOException {
        server.start();
        scheduler.scheduleWithFixedDelay(this::checkBrokers, config.checkIntervalMs(), config.checkIntervalMs(),
                TimeUnit.MILLISECONDS);
        LOG.info("controller listening on port {} with {} partitions", server.boundPort(), state.partitions.size());
    }

    public int port() {
        return server.boundPort();
    }

    private void handle(RequestContext ctx, ProtocolReader body) {
        switch (ctx.header().apiKey()) {
            case HEARTBEAT -> ctx.respond(onHeartbeat(HeartbeatRequest.read(body)));
            case CREATE_TOPIC -> ctx.respond(createTopic(CreateTopicRequest.read(body)));
            case ALTER_ISR -> ctx.respond(alterIsr(AlterIsrRequest.read(body)));
            case METADATA -> ctx.respond(describe(MetadataRequest.read(body).topics()));
            default -> ctx.closeConnection();
        }
    }

    HeartbeatResponse onHeartbeat(HeartbeatRequest req) {
        boolean pushToThis = false;
        boolean pushAll = false;
        long epoch;
        long version;
        synchronized (this) {
            BrokerEntry b = brokers.computeIfAbsent(req.brokerId(), BrokerEntry::new);
            boolean registering = !b.alive || b.epoch < 0 || req.brokerEpoch() != b.epoch;
            if (registering) {
                if (b.alive && b.epoch >= 0) {
                    // Restarted faster than the session timeout: whatever it led is suspect, so run
                    // the same failover as if we'd noticed it die.
                    LOG.info("broker {} re-registered with a new process", b.id);
                    onBrokerFailure(b);
                }
                b.epoch = ++state.brokerEpochCounter;
                b.alive = true;
                b.host = req.host();
                b.port = req.port();
                b.lastHeartbeatNanos = System.nanoTime();
                LOG.info("broker {} registered at {}:{} with broker epoch {}", b.id, b.host, b.port, b.epoch);
                onBrokerStartup(b);
                pushAll = true;
            } else {
                b.lastHeartbeatNanos = System.nanoTime();
                if (req.metadataVersion() < state.metadataVersion) {
                    pushToThis = true;
                }
            }
            epoch = b.epoch;
            version = state.metadataVersion;
        }
        if (pushAll) {
            pushToAll();
        } else if (pushToThis) {
            pushTo(req.brokerId());
        }
        return new HeartbeatResponse(Errors.NONE, epoch, version);
    }

    /** Elects the restarted broker for partitions that went offline because it was their last ISR member. */
    private void onBrokerStartup(BrokerEntry b) {
        for (PartitionState ps : new ArrayList<>(state.partitions.values())) {
            if (ps.leader() == PartitionState.NO_LEADER && ps.isr().contains(b.id)) {
                put(new PartitionState(ps.topic(), ps.partition(), b.id, ps.leaderEpoch() + 1, ps.partitionEpoch() + 1,
                        ps.replicas(), ps.isr()));
                LOG.info("{}-{}: broker {} back, elected leader", ps.topic(), ps.partition(), b.id);
            }
        }
        // Always bump: the live broker list itself changed.
        bumpAndSave(true);
    }

    private void checkBrokers() {
        boolean changed = false;
        synchronized (this) {
            long now = System.nanoTime();
            for (BrokerEntry b : brokers.values()) {
                if (b.alive && now - b.lastHeartbeatNanos > TimeUnit.MILLISECONDS.toNanos(config.sessionTimeoutMs())) {
                    LOG.warn("broker {} missed heartbeats for {} ms, marking dead", b.id, config.sessionTimeoutMs());
                    onBrokerFailure(b);
                    changed = true;
                }
            }
        }
        if (changed) {
            pushToAll();
        }
    }

    /**
     * Moves leadership off a dead broker. The new leader is the first live ISR member in replica
     * order and gets a higher leader epoch, which fences the old leader if it is actually still
     * running somewhere. With no live ISR member the partition goes offline rather than electing
     * an out-of-sync replica.
     */
    private void onBrokerFailure(BrokerEntry dead) {
        dead.alive = false;
        for (PartitionState ps : new ArrayList<>(state.partitions.values())) {
            if (ps.leader() == dead.id) {
                List<Integer> remaining = new ArrayList<>(ps.isr());
                remaining.remove(Integer.valueOf(dead.id));
                Integer next = null;
                for (int id : ps.replicas()) {
                    if (remaining.contains(id) && isAlive(id)) {
                        next = id;
                        break;
                    }
                }
                if (next == null) {
                    LOG.warn("{}-{}: no live in-sync replica, partition offline", ps.topic(), ps.partition());
                    put(new PartitionState(ps.topic(), ps.partition(), PartitionState.NO_LEADER, ps.leaderEpoch() + 1,
                            ps.partitionEpoch() + 1, ps.replicas(), ps.isr()));
                } else {
                    LOG.info("{}-{}: leader {} failed, electing {} (epoch {})", ps.topic(), ps.partition(), dead.id,
                            next, ps.leaderEpoch() + 1);
                    put(new PartitionState(ps.topic(), ps.partition(), next, ps.leaderEpoch() + 1,
                            ps.partitionEpoch() + 1, ps.replicas(), remaining));
                }
            } else if (ps.isr().contains(dead.id) && ps.isr().size() > 1) {
                List<Integer> isr = new ArrayList<>(ps.isr());
                isr.remove(Integer.valueOf(dead.id));
                put(new PartitionState(ps.topic(), ps.partition(), ps.leader(), ps.leaderEpoch(),
                        ps.partitionEpoch() + 1, ps.replicas(), isr));
            }
        }
        bumpAndSave(true);
    }

    private boolean isAlive(int id) {
        BrokerEntry b = brokers.get(id);
        return b != null && b.alive && b.host != null;
    }

    ErrorResponse createTopic(CreateTopicRequest req) {
        String invalid = TopicNames.validate(req.name(), req.name().equals(OffsetsTopic.NAME));
        if (invalid != null) {
            return new ErrorResponse(Errors.INVALID_TOPIC, invalid);
        }
        if (req.partitions() < 1) {
            return new ErrorResponse(Errors.INVALID_PARTITIONS, "need at least one partition");
        }
        synchronized (this) {
            if (state.topicConfigs.containsKey(req.name())) {
                return new ErrorResponse(Errors.TOPIC_ALREADY_EXISTS, req.name());
            }
            List<Integer> live = new ArrayList<>();
            for (BrokerEntry b : brokers.values()) {
                if (isAlive(b.id)) {
                    live.add(b.id);
                }
            }
            int rf = req.replicationFactor() > 0 ? req.replicationFactor()
                    : Math.min(config.defaultReplicationFactor(), live.size());
            if (rf < 1 || rf > live.size()) {
                return new ErrorResponse(Errors.INVALID_REPLICATION_FACTOR,
                        "replication factor " + rf + " but " + live.size() + " live brokers");
            }
            int start = ThreadLocalRandom.current().nextInt(live.size());
            for (int p = 0; p < req.partitions(); p++) {
                List<Integer> replicas = new ArrayList<>(rf);
                for (int i = 0; i < rf; i++) {
                    replicas.add(live.get((start + p + i) % live.size()));
                }
                put(new PartitionState(req.name(), p, replicas.get(0), 0, 0, replicas, replicas));
            }
            state.topicConfigs.put(req.name(), Map.copyOf(req.configs()));
            bumpAndSave(true);
            LOG.info("created topic {} with {} partitions, replication factor {}", req.name(), req.partitions(), rf);
        }
        pushToAll();
        return new ErrorResponse(Errors.NONE, null);
    }

    AlterIsrResponse alterIsr(AlterIsrRequest req) {
        List<AlterIsrResponse.PartitionResult> out = new ArrayList<>();
        boolean changed = false;
        synchronized (this) {
            BrokerEntry sender = brokers.get(req.brokerId());
            if (sender == null || !sender.alive || sender.epoch != req.brokerEpoch()) {
                return new AlterIsrResponse(Errors.STALE_BROKER_EPOCH, List.of());
            }
            for (AlterIsrRequest.PartitionData pd : req.partitions()) {
                TopicPartition tp = new TopicPartition(pd.topic(), pd.partition());
                PartitionState ps = state.partitions.get(tp);
                Errors e = validateIsrChange(req.brokerId(), pd, ps);
                if (e != Errors.NONE) {
                    out.add(new AlterIsrResponse.PartitionResult(pd.topic(), pd.partition(), e,
                            ps == null ? -1 : ps.leaderEpoch(), ps == null ? -1 : ps.partitionEpoch(),
                            ps == null ? List.of() : ps.isr()));
                    continue;
                }
                PartitionState next = new PartitionState(ps.topic(), ps.partition(), ps.leader(), ps.leaderEpoch(),
                        ps.partitionEpoch() + 1, ps.replicas(), pd.newIsr());
                put(next);
                changed = true;
                LOG.info("{}: ISR {} -> {}", tp, ps.isr(), next.isr());
                out.add(new AlterIsrResponse.PartitionResult(pd.topic(), pd.partition(), Errors.NONE,
                        next.leaderEpoch(), next.partitionEpoch(), next.isr()));
            }
            if (changed) {
                bumpAndSave(true);
            }
        }
        if (changed) {
            pushToAll();
        }
        return new AlterIsrResponse(Errors.NONE, out);
    }

    private Errors validateIsrChange(int brokerId, AlterIsrRequest.PartitionData pd, PartitionState ps) {
        if (ps == null) {
            return Errors.UNKNOWN_TOPIC_OR_PARTITION;
        }
        if (ps.leader() != brokerId || pd.leaderEpoch() != ps.leaderEpoch()) {
            return Errors.FENCED_LEADER_EPOCH;
        }
        if (pd.partitionEpoch() != ps.partitionEpoch()) {
            return Errors.INVALID_UPDATE_VERSION;
        }
        if (!pd.newIsr().contains(brokerId) || !ps.replicas().containsAll(pd.newIsr())) {
            return Errors.INVALID_REQUEST;
        }
        for (int id : pd.newIsr()) {
            if (!ps.isr().contains(id) && !isAlive(id)) {
                return Errors.INVALID_REQUEST;
            }
        }
        return Errors.NONE;
    }

    synchronized MetadataResponse describe(List<String> topics) {
        List<BrokerInfo> live = liveBrokers();
        Set<String> wanted = topics.isEmpty() ? state.topicConfigs.keySet() : new HashSet<>(topics);
        List<MetadataResponse.TopicMetadata> out = new ArrayList<>();
        for (String t : wanted) {
            List<PartitionState> parts = state.partitionsOf(t);
            out.add(new MetadataResponse.TopicMetadata(t, parts.isEmpty() ? Errors.UNKNOWN_TOPIC_OR_PARTITION
                    : Errors.NONE, parts));
        }
        return new MetadataResponse(live, out);
    }

    private List<BrokerInfo> liveBrokers() {
        List<BrokerInfo> live = new ArrayList<>();
        for (BrokerEntry b : brokers.values()) {
            if (isAlive(b.id)) {
                live.add(b.info());
            }
        }
        live.sort(Comparator.comparingInt(BrokerInfo::id));
        return live;
    }

    private void put(PartitionState ps) {
        state.partitions.put(new TopicPartition(ps.topic(), ps.partition()), ps);
    }

    private void bumpAndSave(boolean changed) {
        if (!changed) {
            return;
        }
        state.metadataVersion++;
        try {
            state.save();
        } catch (IOException e) {
            // Without durable state we could hand out an epoch twice after a restart.
            throw new UncheckedIOException("failed to persist controller state", e);
        }
    }

    private synchronized LeaderAndIsrRequest snapshot() {
        List<TopicConfig> configs = new ArrayList<>();
        state.topicConfigs.forEach((t, c) -> configs.add(new TopicConfig(t, c)));
        return new LeaderAndIsrRequest(CONTROLLER_ID, state.metadataVersion, liveBrokers(),
                new ArrayList<>(state.partitions.values()), configs);
    }

    private void pushToAll() {
        LeaderAndIsrRequest req = snapshot();
        for (BrokerInfo b : req.liveBrokers()) {
            send(b, req);
        }
    }

    private void pushTo(int brokerId) {
        LeaderAndIsrRequest req = snapshot();
        for (BrokerInfo b : req.liveBrokers()) {
            if (b.id() == brokerId) {
                send(b, req);
            }
        }
    }

    private void send(BrokerInfo b, LeaderAndIsrRequest req) {
        client.send(b.address(), req, PartitionErrorsResponse::read).whenComplete((resp, err) -> {
            if (err != null) {
                LOG.debug("LeaderAndIsr to broker {} failed: {}", b.id(), err.toString());
            } else if (!resp.partitions().isEmpty()) {
                LOG.info("broker {} rejected stale state for {}", b.id(), resp.partitions());
            }
        });
    }

    /** Current state of one partition, for tests and tooling. */
    public synchronized PartitionState partitionState(TopicPartition tp) {
        return state.partitions.get(tp);
    }

    public synchronized boolean isBrokerAlive(int id) {
        return isAlive(id);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        server.close();
        client.close();
        handlers.shutdownNow();
    }
}
