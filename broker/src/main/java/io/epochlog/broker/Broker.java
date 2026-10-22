package io.epochlog.broker;

import io.epochlog.clock.HybridLogicalClock;
import io.epochlog.common.Config;
import io.epochlog.common.Time;
import io.epochlog.common.TopicPartition;
import io.epochlog.concurrent.GroupCommitter;
import io.epochlog.network.NetworkClient;
import io.epochlog.network.SocketServer;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.OffsetsTopic;
import io.epochlog.protocol.TopicNames;
import io.epochlog.protocol.message.BrokerInfo;
import io.epochlog.protocol.message.CreateTopicRequest;
import io.epochlog.protocol.message.ErrorResponse;
import io.epochlog.protocol.message.LeaderAndIsrRequest;
import io.epochlog.protocol.message.PartitionErrorsResponse;
import io.epochlog.protocol.message.PartitionState;
import io.epochlog.replication.ReplicaFetcherManager;
import io.epochlog.replication.ReplicaFetcherThread;
import io.epochlog.storage.Log;
import io.epochlog.storage.LogConfig;
import io.epochlog.storage.LogManager;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A broker process: wires storage, the replica manager, the clock and the network server
 * together and owns their lifecycle. Runs standalone (it leads every partition and creates topics
 * itself) or under a controller, which assigns leaders and followers.
 *
 * <p>Thread-safety: {@link #start} and {@link #close} must not race each other; everything else
 * is thread-safe.
 */
public class Broker implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(Broker.class);

    protected final BrokerConfig config;
    protected final Time time;
    protected final HybridLogicalClock clock;
    protected final LogManager logManager;
    protected final GroupCommitter committer;
    protected final MetadataCache metadataCache = new MetadataCache();
    protected ReplicaManager replicaManager;
    private final ProducerIdManager producerIds;
    private OffsetCoordinator offsetCoordinator;
    private SocketServer server;
    private ExecutorService handlerExecutor;
    private final ScheduledExecutorService scheduler;
    private final UnaryOperator<InetSocketAddress> resolver;
    private final AtomicBoolean offsetsTopicRequested = new AtomicBoolean();
    private NetworkClient controllerClient;
    private NetworkClient replicationClient;
    private ControllerChannel controllerChannel;
    private ReplicaFetcherManager fetchers;
    private volatile boolean started;

    /**
     * @param resolver maps an address this broker wants to connect to onto the one it actually
     *     dials; tests use it to route traffic through fault-injecting proxies
     */
    public Broker(BrokerConfig config, Time time, LongSupplier hlcPhysicalMillis,
            UnaryOperator<InetSocketAddress> resolver) {
        this.config = config;
        this.resolver = resolver;
        this.time = time;
        this.clock = new HybridLogicalClock(hlcPhysicalMillis, config.maxClockDriftMs());
        this.logManager = new LogManager(config.dataDir(), config.logDefaults(), time,
                config.retentionCheckIntervalMs(), config.cleanerIntervalMs());
        this.committer = config.groupCommit()
                ? new GroupCommitter("group-commit-" + config.brokerId(), config.groupCommitMaxWaitMicros(),
                        TimeUnit.MICROSECONDS, config.groupCommitMaxBatch())
                : null;
        this.producerIds = new ProducerIdManager(config.dataDir(), config.brokerId());
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("broker-" + config.brokerId() + "-scheduler").daemon().factory());
    }

    public Broker(BrokerConfig config, Time time, LongSupplier hlcPhysicalMillis) {
        this(config, time, hlcPhysicalMillis, UnaryOperator.identity());
    }

    public Broker(BrokerConfig config) {
        this(config, Time.SYSTEM, System::currentTimeMillis);
    }

    protected ReplicaManager createReplicaManager() {
        return new ReplicaManager(config, logManager, clock, committer, time);
    }

    public void start() throws IOException {
        logManager.startup();
        replicaManager = createReplicaManager();
        if (!config.standalone()) {
            controllerClient = new NetworkClient("broker-" + config.brokerId() + "-controller",
                    "broker-" + config.brokerId(), config.requestTimeoutMs(), resolver);
            replicationClient = new NetworkClient("broker-" + config.brokerId() + "-replication",
                    "broker-" + config.brokerId(), config.requestTimeoutMs(), resolver);
            controllerChannel = new ControllerChannel(config.brokerId(), config.controller(), controllerClient,
                    this::self, metadataCache::version, config.heartbeatIntervalMs(), config.sessionTimeoutMs());
            fetchers = new ReplicaFetcherManager(config.brokerId(), replicationClient,
                    new ReplicaFetcherThread.Settings(config.replicaFetchMaxWaitMs(), config.replicaFetchMaxBytes(),
                            config.replicaFetchMaxBytes(), 200, Math.max(5_000, config.replicaFetchMaxWaitMs() * 4L)),
                    () -> controllerChannel.brokerEpoch());
            replicaManager.enableClusterMode(controllerChannel, fetchers, metadataCache);
        }
        offsetCoordinator = new OffsetCoordinator(replicaManager, metadataCache, this::ensureOffsetsTopic,
                (int) config.requestTimeoutMs());
        handlerExecutor = config.virtualHandlerThreads()
                ? Executors.newVirtualThreadPerTaskExecutor()
                : Executors.newFixedThreadPool(config.handlerThreads(),
                        Thread.ofPlatform().name("handler-" + config.brokerId() + "-", 0).daemon().factory());
        server = new SocketServer("broker-" + config.brokerId(), config.host(), config.port(), config.networkThreads(),
                config.maxRequestBytes(), new BrokerApis(this), handlerExecutor);
        server.start();
        onStartup();
        scheduler.scheduleWithFixedDelay(replicaManager::checkpointHighWatermarks, 5, 5, TimeUnit.SECONDS);
        started = true;
        LOG.info("broker {} started on port {} ({}, {} handler threads)", config.brokerId(), port(),
                config.standalone() ? "standalone" : "cluster mode",
                config.virtualHandlerThreads() ? "virtual" : config.handlerThreads() + " platform");
    }

    /**
     * Standalone mode leads every partition found on disk. In cluster mode the partitions are
     * opened but take no role until the controller says so.
     */
    protected void onStartup() throws IOException {
        if (config.standalone()) {
            for (Log log : logManager.all()) {
                becomeStandaloneLeader(log.topicPartition(), log.config());
            }
            return;
        }
        for (Log log : logManager.all()) {
            replicaManager.getOrCreatePartition(log.topicPartition(), log.config());
        }
        controllerChannel.start();
        long period = Math.max(100, config.replicaLagTimeMaxMs() / 2);
        scheduler.scheduleWithFixedDelay(replicaManager::maintainIsr, period, period, TimeUnit.MILLISECONDS);
    }

    private void becomeStandaloneLeader(TopicPartition tp, LogConfig cfg) throws IOException {
        Partition p = replicaManager.getOrCreatePartition(tp, cfg);
        int epoch = Math.max(p.log().latestEpoch() + 1, p.leaderEpoch() + 1);
        List<Integer> self = List.of(config.brokerId());
        p.becomeLeader(epoch, 0, self, self).join();
        metadataCache.putLocal(self(), List.of(new PartitionState(tp.topic(), tp.partition(), config.brokerId(),
                epoch, 0, self, self)), tp.topic(), cfg.toMap());
    }

    public BrokerInfo self() {
        return new BrokerInfo(config.brokerId(), config.advertisedHost(), port());
    }

    public int port() {
        return server == null ? config.port() : server.boundPort();
    }

    public CompletableFuture<ErrorResponse> createTopic(CreateTopicRequest req) {
        String invalid = TopicNames.validate(req.name(), req.name().equals(OffsetsTopic.NAME));
        if (invalid != null) {
            return CompletableFuture.completedFuture(new ErrorResponse(Errors.INVALID_TOPIC, invalid));
        }
        if (req.partitions() < 1) {
            return CompletableFuture.completedFuture(new ErrorResponse(Errors.INVALID_PARTITIONS,
                    "need at least one partition"));
        }
        if (!config.standalone()) {
            return controllerChannel.createTopic(req);
        }
        return createTopicStandalone(req);
    }

    private synchronized CompletableFuture<ErrorResponse> createTopicStandalone(CreateTopicRequest req) {
        if (req.replicationFactor() > 1) {
            return CompletableFuture.completedFuture(new ErrorResponse(Errors.INVALID_REPLICATION_FACTOR,
                    "a standalone broker can only host replication factor 1"));
        }
        if (metadataCache.topicExists(req.name())) {
            return CompletableFuture.completedFuture(new ErrorResponse(Errors.TOPIC_ALREADY_EXISTS, req.name()));
        }
        LogConfig cfg = LogConfig.from(new Config(config.logDefaults().toMap()).withAll(req.configs()));
        try {
            for (int i = 0; i < req.partitions(); i++) {
                becomeStandaloneLeader(new TopicPartition(req.name(), i), cfg);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        LOG.info("created topic {} with {} partitions", req.name(), req.partitions());
        return CompletableFuture.completedFuture(new ErrorResponse(Errors.NONE, null));
    }

    protected void ensureOffsetsTopic() {
        if (metadataCache.topicExists(OffsetsTopic.NAME) || !offsetsTopicRequested.compareAndSet(false, true)) {
            return;
        }
        createTopic(new CreateTopicRequest(OffsetsTopic.NAME, config.offsetsTopicPartitions(), (short) -1,
                OffsetCoordinator.topicConfigs()))
                .whenComplete((r, err) -> offsetsTopicRequested.set(false));
    }

    public PartitionErrorsResponse handleLeaderAndIsr(LeaderAndIsrRequest req) throws IOException {
        if (config.standalone()) {
            return new PartitionErrorsResponse(Errors.NOT_CONTROLLER, List.of());
        }
        return replicaManager.applyLeaderAndIsr(req);
    }

    long nextProducerId() throws IOException {
        return producerIds.nextProducerId();
    }

    public ReplicaManager replicaManager() {
        return replicaManager;
    }

    public MetadataCache metadataCache() {
        return metadataCache;
    }

    OffsetCoordinator offsetCoordinator() {
        return offsetCoordinator;
    }

    public HybridLogicalClock clock() {
        return clock;
    }

    public BrokerConfig config() {
        return config;
    }

    public boolean isStarted() {
        return started;
    }

    protected void stopBackground() {
        scheduler.shutdownNow();
        if (controllerChannel != null) {
            controllerChannel.close();
        }
        if (server != null) {
            server.close();
        }
        if (handlerExecutor != null) {
            handlerExecutor.shutdownNow();
        }
    }

    private void closeClients() {
        if (controllerClient != null) {
            controllerClient.close();
        }
        if (replicationClient != null) {
            replicationClient.close();
        }
    }

    @Override
    public void close() {
        if (!started) {
            return;
        }
        started = false;
        stopBackground();
        if (replicaManager != null) {
            replicaManager.close();
        }
        closeClients();
        if (committer != null) {
            committer.close();
        }
        try {
            logManager.close();
        } catch (IOException e) {
            LOG.warn("error closing logs", e);
        }
        LOG.info("broker {} shut down", config.brokerId());
    }

    /** Stops everything without flushing or checkpointing, like a killed process. */
    public void crash() {
        if (!started) {
            return;
        }
        started = false;
        stopBackground();
        if (replicaManager != null) {
            replicaManager.closeAbruptly();
        }
        closeClients();
        if (committer != null) {
            committer.close();
        }
        try {
            logManager.closeAbruptly();
        } catch (IOException e) {
            LOG.warn("error closing logs", e);
        }
        LOG.info("broker {} crashed", config.brokerId());
    }
}
