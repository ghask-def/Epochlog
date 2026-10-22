package io.epochlog.broker;

import io.epochlog.clock.HybridLogicalClock;
import io.epochlog.common.Config;
import io.epochlog.common.HybridTime;
import io.epochlog.common.RecordBatch;
import io.epochlog.common.Time;
import io.epochlog.common.TopicPartition;
import io.epochlog.concurrent.DelayedOperationPurgatory;
import io.epochlog.concurrent.GroupCommitter;
import io.epochlog.protocol.ApiKey;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.MemoryRecords;
import io.epochlog.protocol.Records;
import io.epochlog.protocol.message.BrokerInfo;
import io.epochlog.protocol.message.FetchRequest;
import io.epochlog.protocol.message.FetchResponse;
import io.epochlog.protocol.message.LeaderAndIsrRequest;
import io.epochlog.protocol.message.ListOffsetsRequest;
import io.epochlog.protocol.message.ListOffsetsResponse;
import io.epochlog.protocol.message.OffsetForLeaderEpochRequest;
import io.epochlog.protocol.message.OffsetForLeaderEpochResponse;
import io.epochlog.protocol.message.PartitionErrorsResponse;
import io.epochlog.protocol.message.PartitionState;
import io.epochlog.protocol.message.TopicConfig;
import io.epochlog.protocol.message.ProduceRequest;
import io.epochlog.protocol.message.ProduceResponse;
import io.epochlog.replication.ReplicaFetcherManager;
import io.epochlog.storage.FileSlice;
import io.epochlog.storage.LogAppendInfo;
import io.epochlog.storage.LogConfig;
import io.epochlog.storage.LogManager;
import io.epochlog.storage.OffsetAndEpoch;
import io.epochlog.storage.TimestampAndOffset;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns the partitions hosted on this broker and implements the data-plane requests against them:
 * produce (with the acks=all purgatory), fetch (with long polling) and offset lookups.
 *
 * <p>Thread-safety: thread-safe.
 */
public class ReplicaManager implements Partition.Listener, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicaManager.class);

    protected final BrokerConfig config;
    protected final LogManager logManager;
    protected final HybridLogicalClock clock;
    protected final GroupCommitter committer;
    protected final Time time;
    protected final ConcurrentHashMap<TopicPartition, Partition> partitions = new ConcurrentHashMap<>();
    private final DelayedOperationPurgatory<DelayedProduce> producePurgatory;
    private final DelayedOperationPurgatory<DelayedFetch> fetchPurgatory;
    private volatile ControllerChannel controllerChannel;
    private volatile ReplicaFetcherManager fetchers;
    private volatile MetadataCache metadataCache;

    public ReplicaManager(BrokerConfig config, LogManager logManager, HybridLogicalClock clock, GroupCommitter committer,
            Time time) {
        this.config = config;
        this.logManager = logManager;
        this.clock = clock;
        this.committer = committer;
        this.time = time;
        this.producePurgatory = new DelayedOperationPurgatory<>("produce-purgatory", time, 1000);
        this.fetchPurgatory = new DelayedOperationPurgatory<>("fetch-purgatory", time, 1000);
        logManager.setRetentionUpperBound(log -> {
            Partition p = partitions.get(log.topicPartition());
            return p == null ? 0 : p.highWatermark();
        });
    }

    public Optional<Partition> partition(TopicPartition tp) {
        return Optional.ofNullable(partitions.get(tp));
    }

    public Collection<Partition> allPartitions() {
        return partitions.values();
    }

    public Partition getOrCreatePartition(TopicPartition tp, LogConfig logConfig) throws IOException {
        Partition existing = partitions.get(tp);
        if (existing != null) {
            return existing;
        }
        synchronized (partitions) {
            existing = partitions.get(tp);
            if (existing != null) {
                return existing;
            }
            Partition p = new Partition(tp, logManager.getOrCreate(tp, logConfig), config.brokerId(), clock, committer,
                    this, controllerChannel, time);
            partitions.put(tp, p);
            return p;
        }
    }

    @Override
    public void onHighWatermarkChanged(TopicPartition tp) {
        producePurgatory.checkAndComplete(tp);
        fetchPurgatory.checkAndComplete(tp);
    }

    @Override
    public void onLogEndOffsetChanged(TopicPartition tp) {
        fetchPurgatory.checkAndComplete(tp);
    }

    /** Switches on cluster mode: ISR changes go to the controller and followers fetch from leaders. */
    void enableClusterMode(ControllerChannel channel, ReplicaFetcherManager fetcherManager, MetadataCache cache) {
        this.controllerChannel = channel;
        this.fetchers = fetcherManager;
        this.metadataCache = cache;
    }

    protected Errors checkProduceAllowed(Partition p) {
        if (!p.isLeader()) {
            return Errors.NOT_LEADER_OR_FOLLOWER;
        }
        ControllerChannel channel = controllerChannel;
        if (channel != null && !channel.leaseValid()) {
            // We may already have been replaced; don't take writes the new leader won't have.
            return Errors.NOT_LEADER_OR_FOLLOWER;
        }
        return Errors.NONE;
    }

    /** Appends every partition's batches and answers per the request's acks. */
    public void handleProduce(ProduceRequest req, Consumer<ProduceResponse> respond) {
        short acks = req.acks();
        if (acks != 0 && acks != 1 && acks != -1) {
            List<ProduceResponse.PartitionResult> bad = new ArrayList<>();
            for (ProduceRequest.PartitionData pd : req.partitions()) {
                bad.add(new ProduceResponse.PartitionResult(pd.topic(), pd.partition(), Errors.INVALID_REQUEST, -1, -1));
            }
            respond.accept(new ProduceResponse(bad));
            return;
        }
        int n = req.partitions().size();
        List<CompletableFuture<AppendOutcome>> futures = new ArrayList<>(n);
        List<Partition> targets = new ArrayList<>(n);
        for (ProduceRequest.PartitionData pd : req.partitions()) {
            TopicPartition tp = new TopicPartition(pd.topic(), pd.partition());
            Partition p = partitions.get(tp);
            targets.add(p);
            futures.add(appendOne(tp, p, pd.records(), acks));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).whenComplete((v, err) -> {
            List<AppendOutcome> outcomes = new ArrayList<>(n);
            List<ProduceResponse.PartitionResult> results = new ArrayList<>(n);
            for (CompletableFuture<AppendOutcome> f : futures) {
                AppendOutcome o = f.join();
                outcomes.add(o);
                results.add(o.result());
            }
            if (acks != -1) {
                respond.accept(new ProduceResponse(results));
                return;
            }
            List<DelayedProduce.PartitionStatus> pending = new ArrayList<>();
            List<ProduceResponse.PartitionResult> done = new ArrayList<>();
            List<TopicPartition> keys = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                ProduceResponse.PartitionResult r = results.get(i);
                if (r.error() != Errors.NONE) {
                    done.add(r);
                } else {
                    long lastOffset = outcomes.get(i).lastOffset();
                    pending.add(new DelayedProduce.PartitionStatus(targets.get(i), lastOffset + 1, r));
                    keys.add(targets.get(i).topicPartition());
                }
            }
            DelayedProduce op = new DelayedProduce(req.timeoutMs(), pending, done, config.minInsyncReplicas(), respond);
            producePurgatory.tryCompleteElseWatch(op, keys);
        });
    }

    private record AppendOutcome(ProduceResponse.PartitionResult result, long lastOffset) {
        static AppendOutcome failed(TopicPartition tp, Errors e) {
            return new AppendOutcome(ReplicaManager.result(tp, e), -1);
        }
    }

    private CompletableFuture<AppendOutcome> appendOne(TopicPartition tp, Partition p,
            MemoryRecords records, short acks) {
        if (p == null) {
            return CompletableFuture.completedFuture(AppendOutcome.failed(tp, Errors.UNKNOWN_TOPIC_OR_PARTITION));
        }
        Errors allowed = checkProduceAllowed(p);
        if (allowed != Errors.NONE) {
            return CompletableFuture.completedFuture(AppendOutcome.failed(tp, allowed));
        }
        if (acks == -1 && p.isr().size() < config.minInsyncReplicas()) {
            return CompletableFuture.completedFuture(AppendOutcome.failed(tp, Errors.NOT_ENOUGH_REPLICAS));
        }
        List<RecordBatch> batches = records == null ? List.of() : records.batches();
        if (batches.isEmpty()) {
            return CompletableFuture.completedFuture(AppendOutcome.failed(tp, Errors.CORRUPT_MESSAGE));
        }
        List<CompletableFuture<LogAppendInfo>> appends = new ArrayList<>(batches.size());
        for (RecordBatch b : batches) {
            appends.add(p.appendAsLeader(b));
        }
        return CompletableFuture.allOf(appends.toArray(new CompletableFuture<?>[0])).handle((v, err) -> {
            if (err != null) {
                return AppendOutcome.failed(tp, ErrorMapping.of(err));
            }
            LogAppendInfo first = appends.get(0).join();
            long last = first.lastOffset();
            for (CompletableFuture<LogAppendInfo> a : appends) {
                last = Math.max(last, a.join().lastOffset());
            }
            long appendTime = first.maxTimestamp() - (first.lastOffset() - first.firstOffset());
            return new AppendOutcome(new ProduceResponse.PartitionResult(tp.topic(), tp.partition(), Errors.NONE,
                    first.firstOffset(), appendTime), last);
        });
    }

    private static ProduceResponse.PartitionResult result(TopicPartition tp, Errors e) {
        return new ProduceResponse.PartitionResult(tp.topic(), tp.partition(), e, -1, -1);
    }

    /**
     * Handles consumer fetches and, through {@link #onReplicaFetch}, follower fetches. Responds
     * immediately if data is available or the wait is zero, otherwise parks in the fetch purgatory.
     */
    public void handleFetch(FetchRequest req, Consumer<FetchResponse> respond) {
        boolean replica = req.apiKey() == ApiKey.REPLICA_FETCH;
        if (replica) {
            onReplicaFetch(req);
        }
        FetchResponse immediate = readAll(req, replica);
        int bytes = 0;
        boolean anyError = false;
        for (FetchResponse.PartitionData pd : immediate.partitions()) {
            bytes += pd.records() == null ? 0 : pd.records().sizeInBytes();
            anyError |= pd.error() != Errors.NONE;
        }
        if (req.maxWaitMs() <= 0 || bytes >= Math.max(1, req.minBytes()) || anyError) {
            respond.accept(immediate);
            return;
        }
        List<DelayedFetch.Watch> watches = new ArrayList<>();
        List<TopicPartition> keys = new ArrayList<>();
        for (FetchRequest.PartitionData pd : req.partitions()) {
            TopicPartition tp = new TopicPartition(pd.topic(), pd.partition());
            watches.add(new DelayedFetch.Watch(partitions.get(tp), pd.fetchOffset()));
            keys.add(tp);
        }
        DelayedFetch op = new DelayedFetch(req.maxWaitMs(), watches, replica, () -> respond.accept(readAll(req, replica)));
        fetchPurgatory.tryCompleteElseWatch(op, keys);
    }

    /** Records follower progress before the read, so the response carries an up-to-date watermark. */
    protected void onReplicaFetch(FetchRequest req) {
        for (FetchRequest.PartitionData pd : req.partitions()) {
            Partition p = partitions.get(new TopicPartition(pd.topic(), pd.partition()));
            if (p != null && checkFetchEpoch(p, pd.currentLeaderEpoch()) == Errors.NONE) {
                p.onFollowerFetch(req.replicaId(), pd.fetchOffset());
            }
        }
    }

    /** Drops lagging followers from every ISR this broker leads. */
    public void maintainIsr() {
        for (Partition p : partitions.values()) {
            if (p.isLeader()) {
                p.maybeShrinkIsr(config.replicaLagTimeMaxMs());
            }
        }
    }

    /**
     * Applies a controller snapshot: becomes leader or follower for partitions whose leader epoch
     * moved, updates the ISR where only that changed, and rejects anything older than what we
     * already have.
     */
    public synchronized PartitionErrorsResponse applyLeaderAndIsr(LeaderAndIsrRequest req) throws IOException {
        if (!metadataCache.update(req.metadataVersion(), req.controllerId(), req.liveBrokers(), req.partitions(),
                req.topicConfigs())) {
            return new PartitionErrorsResponse(Errors.NONE, List.of());
        }
        Map<String, Map<String, String>> topicConfigs = new HashMap<>();
        for (TopicConfig tc : req.topicConfigs()) {
            topicConfigs.put(tc.topic(), tc.configs());
        }
        Map<Integer, BrokerInfo> brokers = new HashMap<>();
        for (BrokerInfo b : req.liveBrokers()) {
            brokers.put(b.id(), b);
        }
        List<PartitionErrorsResponse.PartitionError> errors = new ArrayList<>();
        List<CompletableFuture<Void>> transitions = new ArrayList<>();
        int self = config.brokerId();
        for (PartitionState ps : req.partitions()) {
            if (!ps.replicas().contains(self)) {
                continue;
            }
            TopicPartition tp = new TopicPartition(ps.topic(), ps.partition());
            LogConfig cfg = LogConfig.from(new Config(logManager.defaults().toMap())
                    .withAll(topicConfigs.getOrDefault(tp.topic(), Map.of())));
            Partition p = getOrCreatePartition(tp, cfg);
            if (ps.leaderEpoch() < p.leaderEpoch()) {
                errors.add(new PartitionErrorsResponse.PartitionError(tp.topic(), tp.partition(),
                        Errors.FENCED_LEADER_EPOCH));
                continue;
            }
            if (ps.leaderEpoch() == p.leaderEpoch()) {
                if (ps.partitionEpoch() > p.partitionEpoch()) {
                    transitions.add(p.updateIsr(ps.partitionEpoch(), ps.replicas(), ps.isr()));
                }
                continue;
            }
            if (ps.leader() == self) {
                fetchers.remove(tp);
                transitions.add(p.becomeLeader(ps.leaderEpoch(), ps.partitionEpoch(), ps.replicas(), ps.isr()));
            } else {
                BrokerInfo leader = brokers.get(ps.leader());
                transitions.add(p.becomeFollower(ps.leader(), ps.leaderEpoch(), ps.partitionEpoch(), ps.replicas(),
                        ps.isr()).thenRun(() -> {
                            if (leader != null) {
                                fetchers.follow(p, leader);
                            } else {
                                fetchers.remove(tp);
                            }
                        }));
            }
        }
        try {
            CompletableFuture.allOf(transitions.toArray(new CompletableFuture<?>[0])).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("leadership transitions failed", e);
            return new PartitionErrorsResponse(Errors.UNKNOWN, errors);
        }
        return new PartitionErrorsResponse(Errors.NONE, errors);
    }

    protected FetchResponse readAll(FetchRequest req, boolean replica) {
        List<FetchResponse.PartitionData> out = new ArrayList<>(req.partitions().size());
        int budget = req.maxBytes();
        for (FetchRequest.PartitionData pd : req.partitions()) {
            TopicPartition tp = new TopicPartition(pd.topic(), pd.partition());
            Partition p = partitions.get(tp);
            if (p == null) {
                out.add(fetchError(tp, Errors.UNKNOWN_TOPIC_OR_PARTITION));
                continue;
            }
            Errors epochCheck = checkFetchEpoch(p, pd.currentLeaderEpoch());
            if (epochCheck != Errors.NONE) {
                out.add(fetchError(tp, epochCheck));
                continue;
            }
            try {
                int limit = Math.max(0, Math.min(pd.maxBytes(), budget));
                long hw = p.highWatermark();
                FileSlice slice = limit == 0 ? null
                        : replica ? p.log().read(pd.fetchOffset(), limit, Long.MAX_VALUE)
                        : p.readCommitted(pd.fetchOffset(), limit);
                Records records = slice == null ? MemoryRecords.EMPTY : new SliceRecords(slice);
                budget -= records.sizeInBytes();
                out.add(new FetchResponse.PartitionData(tp.topic(), tp.partition(), Errors.NONE, hw,
                        p.log().logStartOffset(), p.logEndOffset(), records));
            } catch (Exception e) {
                out.add(fetchError(tp, ErrorMapping.of(e)));
            }
        }
        return new FetchResponse(out);
    }

    protected Errors checkFetchEpoch(Partition p, int requestEpoch) {
        if (!p.isLeader()) {
            return Errors.NOT_LEADER_OR_FOLLOWER;
        }
        if (requestEpoch < 0) {
            return Errors.NONE;
        }
        if (requestEpoch < p.leaderEpoch()) {
            return Errors.FENCED_LEADER_EPOCH;
        }
        if (requestEpoch > p.leaderEpoch()) {
            return Errors.UNKNOWN_LEADER_EPOCH;
        }
        return Errors.NONE;
    }

    private static FetchResponse.PartitionData fetchError(TopicPartition tp, Errors e) {
        return new FetchResponse.PartitionData(tp.topic(), tp.partition(), e, -1, -1, -1, MemoryRecords.EMPTY);
    }

    public ListOffsetsResponse handleListOffsets(ListOffsetsRequest req) {
        List<ListOffsetsResponse.PartitionResult> out = new ArrayList<>();
        for (ListOffsetsRequest.PartitionData pd : req.partitions()) {
            TopicPartition tp = new TopicPartition(pd.topic(), pd.partition());
            Partition p = partitions.get(tp);
            if (p == null || !p.isLeader()) {
                Errors e = p == null ? Errors.UNKNOWN_TOPIC_OR_PARTITION : Errors.NOT_LEADER_OR_FOLLOWER;
                out.add(new ListOffsetsResponse.PartitionResult(tp.topic(), tp.partition(), e, -1, -1));
                continue;
            }
            try {
                long ts = pd.timestamp();
                if (ts == ListOffsetsRequest.LATEST) {
                    out.add(ok(tp, HybridTime.NO_TIMESTAMP, p.highWatermark()));
                } else if (ts == ListOffsetsRequest.EARLIEST) {
                    out.add(ok(tp, HybridTime.NO_TIMESTAMP, p.log().logStartOffset()));
                } else {
                    TimestampAndOffset found = p.offsetForTimestamp(ts);
                    out.add(found == null ? ok(tp, HybridTime.NO_TIMESTAMP, -1) : ok(tp, found.timestamp(), found.offset()));
                }
            } catch (Exception e) {
                out.add(new ListOffsetsResponse.PartitionResult(tp.topic(), tp.partition(), ErrorMapping.of(e), -1, -1));
            }
        }
        return new ListOffsetsResponse(out);
    }

    private static ListOffsetsResponse.PartitionResult ok(TopicPartition tp, long ts, long offset) {
        return new ListOffsetsResponse.PartitionResult(tp.topic(), tp.partition(), Errors.NONE, ts, offset);
    }

    /** Tells a follower where an epoch ends in the leader's log, so it can truncate divergent data. */
    public OffsetForLeaderEpochResponse handleOffsetForLeaderEpoch(OffsetForLeaderEpochRequest req) {
        List<OffsetForLeaderEpochResponse.PartitionResult> out = new ArrayList<>();
        for (OffsetForLeaderEpochRequest.PartitionData pd : req.partitions()) {
            TopicPartition tp = new TopicPartition(pd.topic(), pd.partition());
            Partition p = partitions.get(tp);
            Errors e = p == null ? Errors.UNKNOWN_TOPIC_OR_PARTITION : checkFetchEpoch(p, pd.currentLeaderEpoch());
            if (e != Errors.NONE) {
                out.add(new OffsetForLeaderEpochResponse.PartitionResult(tp.topic(), tp.partition(), e,
                        OffsetAndEpoch.UNDEFINED_EPOCH, OffsetAndEpoch.UNDEFINED_OFFSET));
                continue;
            }
            OffsetAndEpoch end = p.log().endOffsetForEpoch(pd.leaderEpoch());
            out.add(new OffsetForLeaderEpochResponse.PartitionResult(tp.topic(), tp.partition(), Errors.NONE,
                    end.epoch(), end.offset()));
        }
        return new OffsetForLeaderEpochResponse(out);
    }

    public void checkpointHighWatermarks() {
        for (Partition p : partitions.values()) {
            try {
                p.checkpointHighWatermark();
            } catch (IOException e) {
                LOG.warn("failed to checkpoint high watermark for {}", p.topicPartition(), e);
            }
        }
    }

    private void stopFetchers() {
        ReplicaFetcherManager f = fetchers;
        if (f != null) {
            f.shutdown();
        }
    }

    @Override
    public void close() {
        stopFetchers();
        producePurgatory.close();
        fetchPurgatory.close();
        for (Partition p : partitions.values()) {
            p.close();
        }
    }

    public void closeAbruptly() {
        stopFetchers();
        producePurgatory.close();
        fetchPurgatory.close();
        for (Partition p : partitions.values()) {
            p.closeAbruptly();
        }
    }
}
