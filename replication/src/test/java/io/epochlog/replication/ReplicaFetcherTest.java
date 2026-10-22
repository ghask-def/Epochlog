package io.epochlog.replication;

import static org.assertj.core.api.Assertions.assertThat;

import io.epochlog.common.RecordBatch;
import io.epochlog.common.RecordBatchBuilder;
import io.epochlog.common.TopicPartition;
import io.epochlog.network.NetworkClient;
import io.epochlog.network.SocketServer;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReplicaFetcherTest {
    private static final TopicPartition TP = new TopicPartition("r", 0);

    /** A leader that serves batches from a list, all in one epoch. */
    static final class FakeLeader {
        final List<RecordBatch> log = new ArrayList<>();
        volatile int epoch = 5;
        volatile long logStart;
        volatile Errors forcedError = Errors.NONE;
        final List<Long> fetchOffsets = new ArrayList<>();

        synchronized void append(int records) {
            long base = log.isEmpty() ? logStart : log.get(log.size() - 1).nextOffset();
            RecordBatchBuilder b = new RecordBatchBuilder(io.epochlog.common.CompressionType.NONE, base, 64);
            for (int i = 0; i < records; i++) {
                b.append(null, ("v" + (base + i)).getBytes());
            }
            RecordBatch batch = b.leaderEpoch(epoch).build();
            log.add(batch);
        }

        synchronized long end() {
            return log.isEmpty() ? logStart : log.get(log.size() - 1).nextOffset();
        }

        synchronized FetchResponse fetch(FetchRequest req) {
            FetchRequest.PartitionData pd = req.partitions().get(0);
            fetchOffsets.add(pd.fetchOffset());
            if (forcedError != Errors.NONE) {
                return new FetchResponse(List.of(new FetchResponse.PartitionData(TP.topic(), 0, forcedError, -1, -1, -1,
                        MemoryRecords.EMPTY)));
            }
            if (pd.fetchOffset() > end() || pd.fetchOffset() < logStart) {
                return new FetchResponse(List.of(new FetchResponse.PartitionData(TP.topic(), 0,
                        Errors.OFFSET_OUT_OF_RANGE, end(), logStart, end(), MemoryRecords.EMPTY)));
            }
            List<RecordBatch> out = new ArrayList<>();
            for (RecordBatch b : log) {
                if (b.lastOffset() >= pd.fetchOffset()) {
                    out.add(b);
                }
            }
            return new FetchResponse(List.of(new FetchResponse.PartitionData(TP.topic(), 0, Errors.NONE, end(), logStart,
                    end(), MemoryRecords.of(out))));
        }

        synchronized OffsetForLeaderEpochResponse endOffset(OffsetForLeaderEpochRequest req) {
            OffsetForLeaderEpochRequest.PartitionData pd = req.partitions().get(0);
            // This leader's history: epoch 1 covered [0, 10), then epoch 5 from 10 on.
            OffsetAndEpoch answer = pd.leaderEpoch() >= epoch ? new OffsetAndEpoch(end(), epoch)
                    : new OffsetAndEpoch(10, 1);
            return new OffsetForLeaderEpochResponse(List.of(new OffsetForLeaderEpochResponse.PartitionResult(
                    TP.topic(), 0, Errors.NONE, answer.epoch(), answer.offset())));
        }
    }

    /** In-memory follower log. */
    static final class FakeFollower implements FollowerReplica {
        final List<long[]> epochs = new ArrayList<>();
        volatile long leo;
        volatile long hw;
        volatile int leaderEpoch = 5;
        final List<String> events = new ArrayList<>();

        @Override
        public TopicPartition topicPartition() {
            return TP;
        }

        @Override
        public int leaderEpoch() {
            return leaderEpoch;
        }

        @Override
        public long logEndOffset() {
            return leo;
        }

        @Override
        public long highWatermark() {
            return hw;
        }

        @Override
        public int latestLogEpoch() {
            return epochs.isEmpty() ? -1 : (int) epochs.get(epochs.size() - 1)[0];
        }

        @Override
        public OffsetAndEpoch endOffsetForEpoch(int epoch) {
            long[] floor = null;
            long[] higher = null;
            for (long[] e : epochs) {
                if (e[0] <= epoch) {
                    floor = e;
                } else if (higher == null) {
                    higher = e;
                }
            }
            return floor == null ? OffsetAndEpoch.UNDEFINED
                    : new OffsetAndEpoch(higher == null ? leo : higher[1], (int) floor[0]);
        }

        @Override
        public synchronized CompletableFuture<Void> appendAsFollower(MemoryRecords records, long leaderHw, int epoch) {
            for (RecordBatch b : records.batches()) {
                if (b.baseOffset() != leo) {
                    continue;
                }
                if (epochs.isEmpty() || epochs.get(epochs.size() - 1)[0] < b.leaderEpoch()) {
                    epochs.add(new long[] {b.leaderEpoch(), b.baseOffset()});
                }
                leo = b.nextOffset();
            }
            hw = Math.min(leaderHw, leo);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public synchronized CompletableFuture<Void> truncateTo(long offset, int epoch) {
            events.add("truncate:" + offset);
            leo = Math.min(leo, offset);
            epochs.removeIf(e -> e[1] >= leo);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public synchronized CompletableFuture<Void> truncateFullyAndStartAt(long offset, int epoch) {
            events.add("reset:" + offset);
            leo = offset;
            hw = offset;
            epochs.clear();
            return CompletableFuture.completedFuture(null);
        }
    }

    private final FakeLeader leader = new FakeLeader();
    private SocketServer server;
    private NetworkClient client;
    private BrokerInfo leaderInfo;

    @BeforeEach
    void start() throws Exception {
        server = new SocketServer("fake-leader", "127.0.0.1", 0, 1, 1 << 20, (ctx, body) -> {
            ApiKey api = ctx.header().apiKey();
            if (api == ApiKey.REPLICA_FETCH) {
                FetchRequest req = FetchRequest.read(api, body);
                Thread.sleep(5);
                ctx.respond(leader.fetch(req));
            } else if (api == ApiKey.OFFSET_FOR_LEADER_EPOCH) {
                ctx.respond(leader.endOffset(OffsetForLeaderEpochRequest.read(body)));
            } else {
                ctx.closeConnection();
            }
        }, Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        client = new NetworkClient("follower", "follower", 2_000);
        leaderInfo = new BrokerInfo(1, "127.0.0.1", server.boundPort());
    }

    @AfterEach
    void stop() {
        client.close();
        server.close();
    }

    private ReplicaFetcherManager manager() {
        return new ReplicaFetcherManager(2, client, new ReplicaFetcherThread.Settings(50, 1 << 20, 1 << 20, 20, 2_000),
                () -> 7L);
    }

    private static void await(BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("timed out");
            }
            Thread.sleep(10);
        }
    }

    @Test
    void emptyFollowerCatchesUp() throws Exception {
        leader.append(3);
        leader.append(4);
        FakeFollower f = new FakeFollower();
        ReplicaFetcherManager m = manager();
        m.follow(f, leaderInfo);
        await(() -> f.leo == 7 && f.hw == 7);
        leader.append(2);
        await(() -> f.leo == 9);
        assertThat(m.fetcherCount()).isEqualTo(1);
        m.remove(TP);
        assertThat(m.fetcherCount()).isZero();
        m.shutdown();
    }

    @Test
    void divergentSuffixIsTruncatedBeforeFetching() throws Exception {
        leader.append(10);
        leader.append(5);
        // We led epoch 3 at offset 10 and wrote 8 records the new leader never saw.
        FakeFollower f = new FakeFollower();
        f.epochs.add(new long[] {1, 0});
        f.epochs.add(new long[] {3, 10});
        f.leo = 18;
        f.hw = 10;
        ReplicaFetcherManager m = manager();
        m.follow(f, leaderInfo);
        await(() -> f.leo == 15);
        assertThat(f.events).contains("truncate:10");
        m.shutdown();
    }

    @Test
    void followerBehindLogStartResetsItsLog() throws Exception {
        leader.logStart = 100;
        leader.append(5);
        FakeFollower f = new FakeFollower();
        ReplicaFetcherManager m = manager();
        m.follow(f, leaderInfo);
        await(() -> f.leo == 105);
        assertThat(f.events).contains("reset:100");
        m.shutdown();
    }

    @Test
    void fencedResponsesBackOffAndRetry() throws Exception {
        leader.append(2);
        leader.forcedError = Errors.FENCED_LEADER_EPOCH;
        FakeFollower f = new FakeFollower();
        ReplicaFetcherManager m = manager();
        m.follow(f, leaderInfo);
        Thread.sleep(150);
        assertThat(f.leo).isZero();
        leader.forcedError = Errors.NONE;
        await(() -> f.leo == 2);
        m.shutdown();
    }

    @Test
    void movingToANewLeaderRestartsTheFetcher() throws Exception {
        leader.append(1);
        FakeFollower f = new FakeFollower();
        ReplicaFetcherManager m = manager();
        m.follow(f, new BrokerInfo(1, "127.0.0.1", 1));
        m.follow(f, leaderInfo);
        await(() -> f.leo == 1);
        assertThat(m.fetcherCount()).isEqualTo(1);
        m.shutdown();
    }
}
