package io.epochlog.replication;

import io.epochlog.common.TopicPartition;
import io.epochlog.network.NetworkClient;
import io.epochlog.protocol.ApiKey;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.MemoryRecords;
import io.epochlog.protocol.message.BrokerInfo;
import io.epochlog.protocol.message.FetchRequest;
import io.epochlog.protocol.message.FetchResponse;
import io.epochlog.protocol.message.OffsetForLeaderEpochRequest;
import io.epochlog.protocol.message.OffsetForLeaderEpochResponse;
import io.epochlog.storage.OffsetAndEpoch;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pulls data for every partition this broker follows from one leader. A partition starts in the
 * truncating state: before fetching, it asks the leader where its latest epoch ends and cuts off
 * any suffix the leader doesn't have. Then it long-polls {@code ReplicaFetch} and appends what
 * comes back.
 *
 * <p>Thread-safety: {@link #add}, {@link #remove} and {@link #shutdown} are thread-safe; fetching
 * happens on this thread only.
 */
public final class ReplicaFetcherThread extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicaFetcherThread.class);

    /**
     * Fetcher settings.
     *
     * <p>Thread-safety: immutable.
     */
    public record Settings(int maxWaitMs, int maxBytes, int partitionMaxBytes, long backoffMs, long requestTimeoutMs) {}

    private static final class FetchState {
        final FollowerReplica replica;
        final int epoch;
        volatile boolean truncating = true;
        volatile long backoffUntil;

        FetchState(FollowerReplica replica) {
            this.replica = replica;
            this.epoch = replica.leaderEpoch();
        }
    }

    private final int localBrokerId;
    private final BrokerInfo leader;
    private final NetworkClient client;
    private final Settings settings;
    private final LongSupplier brokerEpoch;
    private final Map<TopicPartition, FetchState> partitions = new ConcurrentHashMap<>();
    private volatile boolean running = true;

    public ReplicaFetcherThread(int localBrokerId, BrokerInfo leader, NetworkClient client, Settings settings,
            LongSupplier brokerEpoch) {
        super("replica-fetcher-" + localBrokerId + "-from-" + leader.id());
        setDaemon(true);
        this.localBrokerId = localBrokerId;
        this.leader = leader;
        this.client = client;
        this.settings = settings;
        this.brokerEpoch = brokerEpoch;
    }

    public BrokerInfo leader() {
        return leader;
    }

    public void add(FollowerReplica replica) {
        partitions.put(replica.topicPartition(), new FetchState(replica));
    }

    public void remove(TopicPartition tp) {
        partitions.remove(tp);
    }

    public boolean isEmpty() {
        return partitions.isEmpty();
    }

    @Override
    public void run() {
        while (running) {
            try {
                if (partitions.isEmpty()) {
                    Thread.sleep(50);
                    continue;
                }
                truncateWhereNeeded();
                fetchOnce();
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                if (running) {
                    LOG.debug("fetch from broker {} failed: {}", leader.id(), e.toString());
                    try {
                        Thread.sleep(settings.backoffMs());
                    } catch (InterruptedException ie) {
                        return;
                    }
                }
            }
        }
    }

    private void truncateWhereNeeded() throws Exception {
        long now = System.currentTimeMillis();
        List<FetchState> pending = new ArrayList<>();
        List<OffsetForLeaderEpochRequest.PartitionData> query = new ArrayList<>();
        for (FetchState s : partitions.values()) {
            if (!s.truncating || s.backoffUntil > now) {
                continue;
            }
            int latest = s.replica.latestLogEpoch();
            if (latest < 0) {
                // Nothing local to diverge.
                s.truncating = false;
                continue;
            }
            TopicPartition tp = s.replica.topicPartition();
            pending.add(s);
            query.add(new OffsetForLeaderEpochRequest.PartitionData(tp.topic(), tp.partition(),
                    s.epoch, latest));
        }
        if (query.isEmpty()) {
            return;
        }
        OffsetForLeaderEpochResponse resp = client.send(leader.address(),
                new OffsetForLeaderEpochRequest(localBrokerId, query), OffsetForLeaderEpochResponse::read)
                .get(settings.requestTimeoutMs(), TimeUnit.MILLISECONDS);
        for (int i = 0; i < resp.partitions().size(); i++) {
            OffsetForLeaderEpochResponse.PartitionResult r = resp.partitions().get(i);
            FetchState s = partitions.get(new TopicPartition(r.topic(), r.partition()));
            if (s == null || s != pending.get(i)) {
                continue;
            }
            if (r.error() != Errors.NONE) {
                s.backoffUntil = System.currentTimeMillis() + settings.backoffMs();
                continue;
            }
            int requested = query.get(i).leaderEpoch();
            EpochTruncation.Decision d = EpochTruncation.decide(new OffsetAndEpoch(r.endOffset(), r.leaderEpoch()),
                    requested, s.replica);
            if (d.truncateTo() < s.replica.logEndOffset()) {
                LOG.info("{}: truncating to {} (leader {} says epoch {} ends at {})", s.replica.topicPartition(),
                        d.truncateTo(), leader.id(), r.leaderEpoch(), r.endOffset());
                s.replica.truncateTo(d.truncateTo(), s.epoch).get(settings.requestTimeoutMs(), TimeUnit.MILLISECONDS);
            }
            s.truncating = !d.done();
        }
    }

    private void fetchOnce() throws Exception {
        long now = System.currentTimeMillis();
        List<FetchRequest.PartitionData> parts = new ArrayList<>();
        for (FetchState s : partitions.values()) {
            if (s.truncating || s.backoffUntil > now) {
                continue;
            }
            TopicPartition tp = s.replica.topicPartition();
            parts.add(new FetchRequest.PartitionData(tp.topic(), tp.partition(), s.replica.logEndOffset(),
                    s.epoch, settings.partitionMaxBytes()));
        }
        if (parts.isEmpty()) {
            Thread.sleep(Math.min(settings.backoffMs(), 50));
            return;
        }
        FetchRequest req = new FetchRequest(ApiKey.REPLICA_FETCH, localBrokerId, brokerEpoch.getAsLong(),
                settings.maxWaitMs(), 1, settings.maxBytes(), parts);
        FetchResponse resp = client.send(leader.address(), req, FetchResponse::read)
                .get(settings.requestTimeoutMs(), TimeUnit.MILLISECONDS);
        for (FetchResponse.PartitionData pd : resp.partitions()) {
            FetchState s = partitions.get(new TopicPartition(pd.topic(), pd.partition()));
            if (s == null) {
                continue;
            }
            switch (pd.error()) {
                case NONE -> {
                    MemoryRecords records = pd.records() instanceof MemoryRecords m ? m : MemoryRecords.EMPTY;
                    s.replica.appendAsFollower(records, pd.highWatermark(), s.epoch)
                            .get(settings.requestTimeoutMs(), TimeUnit.MILLISECONDS);
                }
                case OFFSET_OUT_OF_RANGE -> {
                    if (s.replica.logEndOffset() < pd.logStartOffset() || pd.logStartOffset() < 0) {
                        s.replica.truncateFullyAndStartAt(Math.max(0, pd.logStartOffset()), s.epoch)
                                .get(settings.requestTimeoutMs(), TimeUnit.MILLISECONDS);
                    } else {
                        s.truncating = true;
                    }
                }
                default -> {
                    // Leader moved or epochs disagree; wait for the controller to tell us more.
                    s.backoffUntil = System.currentTimeMillis() + settings.backoffMs();
                }
            }
        }
    }

    public void shutdown() {
        running = false;
        interrupt();
        try {
            join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
