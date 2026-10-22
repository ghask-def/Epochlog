package io.epochlog.client;

import io.epochlog.clock.HybridLogicalClock;
import io.epochlog.common.Record;
import io.epochlog.common.RecordBatch;
import io.epochlog.common.TopicPartition;
import io.epochlog.network.NetworkClient;
import io.epochlog.protocol.ApiKey;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.MemoryRecords;
import io.epochlog.protocol.message.BrokerInfo;
import io.epochlog.protocol.message.FetchRequest;
import io.epochlog.protocol.message.FetchResponse;
import io.epochlog.protocol.message.ListOffsetsRequest;
import io.epochlog.protocol.message.ListOffsetsResponse;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Pull-based consumer for explicitly assigned partitions. Reads only committed data (up to the
 * high watermark). Positions start from the group's committed offset if there is one, otherwise
 * from {@code auto.offset.reset}. Each poll returns buffered records and immediately sends the
 * next fetch, so network round trips overlap with processing.
 *
 * <p>Thread-safety: not thread-safe. Use from one thread; the network callbacks only hand
 * completed fetches over through a blocking queue.
 */
public final class Consumer implements AutoCloseable {
    private final ClientConfig config;
    private final NetworkClient network;
    private final ClientMetadata metadata;
    private final OffsetClient offsets;
    private final HybridLogicalClock clock = HybridLogicalClock.system();
    private final LinkedHashMap<TopicPartition, PartitionState> assigned = new LinkedHashMap<>();
    private final BlockingQueue<CompletedFetch> completed = new LinkedBlockingQueue<>();
    private final Set<Integer> nodesWithFetchInFlight = new HashSet<>();

    private static final class PartitionState {
        Long position;
        Long pendingTimestamp;
        boolean checkedCommitted;
        ArrayDeque<ConsumerRecord> buffer = new ArrayDeque<>();
        long bufferEnd = -1;
        boolean fetchInFlight;
    }

    private record CompletedFetch(int nodeId, Map<TopicPartition, Long> requested, FetchResponse response,
            Throwable error) {}

    public Consumer(ClientConfig config) {
        this.config = config;
        this.network = new NetworkClient("consumer-" + config.clientId(), config.clientId(), config.requestTimeoutMs());
        this.metadata = new ClientMetadata(config.bootstrap(), network);
        this.offsets = new OffsetClient(network, metadata, config.requestTimeoutMs(), config.retryBackoffMs());
    }

    public Consumer(Map<String, String> props) {
        this(ClientConfig.from(props));
    }

    public void assign(Collection<TopicPartition> partitions) {
        assigned.keySet().retainAll(partitions);
        for (TopicPartition tp : partitions) {
            assigned.computeIfAbsent(tp, k -> new PartitionState());
        }
    }

    /** Assigns every partition of the topic, waiting for it to exist. */
    public List<TopicPartition> assignTopic(String topic) throws TimeoutException, InterruptedException {
        int n = metadata.awaitTopic(topic, config.requestTimeoutMs());
        List<TopicPartition> tps = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            tps.add(new TopicPartition(topic, i));
        }
        assign(tps);
        return tps;
    }

    public Set<TopicPartition> assignment() {
        return Set.copyOf(assigned.keySet());
    }

    private PartitionState state(TopicPartition tp) {
        PartitionState s = assigned.get(tp);
        if (s == null) {
            throw new IllegalStateException(tp + " is not assigned");
        }
        return s;
    }

    public void seek(TopicPartition tp, long offset) {
        PartitionState s = state(tp);
        s.position = offset;
        s.pendingTimestamp = null;
        s.buffer.clear();
        s.bufferEnd = -1;
    }

    public void seekToBeginning(Collection<TopicPartition> tps) {
        for (TopicPartition tp : tps) {
            seekToTimestampInternal(tp, ListOffsetsRequest.EARLIEST);
        }
    }

    public void seekToEnd(Collection<TopicPartition> tps) {
        for (TopicPartition tp : tps) {
            seekToTimestampInternal(tp, ListOffsetsRequest.LATEST);
        }
    }

    /**
     * Positions at the first committed record whose HLC timestamp is at or after {@code hlc}, or
     * at the end if there is none. Resolved lazily on the next poll.
     */
    public void seekToTimestamp(TopicPartition tp, long hlc) {
        if (hlc < 0) {
            throw new IllegalArgumentException("timestamp must be non-negative");
        }
        seekToTimestampInternal(tp, hlc);
    }

    private void seekToTimestampInternal(TopicPartition tp, long ts) {
        PartitionState s = state(tp);
        s.position = null;
        s.pendingTimestamp = ts;
        s.checkedCommitted = true;
        s.buffer.clear();
        s.bufferEnd = -1;
    }

    /** The offset of the next record poll would return, resolving it if needed. */
    public long position(TopicPartition tp) throws InterruptedException, TimeoutException {
        resolvePositions();
        Long p = state(tp).position;
        if (p == null) {
            throw new TimeoutException("could not resolve position for " + tp);
        }
        return p;
    }

    public List<ConsumerRecord> poll(Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (true) {
            try {
                resolvePositions();
            } catch (TimeoutException e) {
                // leave unresolved partitions for the next round
            }
            drainCompleted(0);
            List<ConsumerRecord> out = drainBuffers();
            sendFetches();
            if (!out.isEmpty()) {
                return out;
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                return List.of();
            }
            drainCompleted(Math.min(remaining, 100));
            out = drainBuffers();
            if (!out.isEmpty()) {
                sendFetches();
                return out;
            }
        }
    }

    private void resolvePositions() throws InterruptedException, TimeoutException {
        Map<TopicPartition, Long> needLookup = new HashMap<>();
        for (Map.Entry<TopicPartition, PartitionState> e : assigned.entrySet()) {
            PartitionState s = e.getValue();
            if (s.position != null) {
                continue;
            }
            if (s.pendingTimestamp == null && config.groupId() != null && !s.checkedCommitted) {
                OptionalLong committed = offsets.fetch(config.groupId(), e.getKey());
                s.checkedCommitted = true;
                if (committed.isPresent()) {
                    s.position = committed.getAsLong();
                    continue;
                }
            }
            if (s.pendingTimestamp == null) {
                switch (config.autoOffsetReset()) {
                    case "earliest" -> s.pendingTimestamp = ListOffsetsRequest.EARLIEST;
                    case "latest" -> s.pendingTimestamp = ListOffsetsRequest.LATEST;
                    default -> throw new IllegalStateException("no position for " + e.getKey()
                            + " and auto.offset.reset=" + config.autoOffsetReset());
                }
            }
            needLookup.put(e.getKey(), s.pendingTimestamp);
        }
        if (needLookup.isEmpty()) {
            return;
        }
        Map<TopicPartition, Long> toEnd = applyLookups(listOffsets(needLookup));
        if (!toEnd.isEmpty()) {
            applyLookups(listOffsets(toEnd));
        }
    }

    /** Sets positions from lookup results; returns partitions with no match, which go to the end. */
    private Map<TopicPartition, Long> applyLookups(Map<TopicPartition, ListOffsetsResponse.PartitionResult> found) {
        Map<TopicPartition, Long> toEnd = new HashMap<>();
        for (Map.Entry<TopicPartition, ListOffsetsResponse.PartitionResult> e : found.entrySet()) {
            PartitionState s = assigned.get(e.getKey());
            if (s == null || s.position != null) {
                continue;
            }
            long offset = e.getValue().offset();
            if (offset < 0) {
                s.pendingTimestamp = ListOffsetsRequest.LATEST;
                toEnd.put(e.getKey(), ListOffsetsRequest.LATEST);
                continue;
            }
            s.position = offset;
            s.pendingTimestamp = null;
        }
        return toEnd;
    }

    /** Looks up offsets, retrying on leader changes until the request timeout. */
    public Map<TopicPartition, ListOffsetsResponse.PartitionResult> listOffsets(Map<TopicPartition, Long> query)
            throws InterruptedException, TimeoutException {
        Map<TopicPartition, ListOffsetsResponse.PartitionResult> out = new HashMap<>();
        Map<TopicPartition, Long> remaining = new HashMap<>(query);
        long deadline = System.currentTimeMillis() + config.requestTimeoutMs();
        while (!remaining.isEmpty()) {
            if (System.currentTimeMillis() > deadline) {
                throw new TimeoutException("list offsets timed out for " + remaining.keySet());
            }
            Map<BrokerInfo, List<ListOffsetsRequest.PartitionData>> byLeader = new HashMap<>();
            boolean missing = false;
            for (Map.Entry<TopicPartition, Long> e : remaining.entrySet()) {
                BrokerInfo leader = metadata.leaderFor(e.getKey()).orElse(null);
                if (leader == null) {
                    missing = true;
                    continue;
                }
                byLeader.computeIfAbsent(leader, k -> new ArrayList<>())
                        .add(new ListOffsetsRequest.PartitionData(e.getKey().topic(), e.getKey().partition(), e.getValue()));
            }
            for (Map.Entry<BrokerInfo, List<ListOffsetsRequest.PartitionData>> e : byLeader.entrySet()) {
                try {
                    ListOffsetsResponse resp = network.send(e.getKey().address(), new ListOffsetsRequest(e.getValue()),
                            ListOffsetsResponse::read).get(config.requestTimeoutMs(), TimeUnit.MILLISECONDS);
                    for (ListOffsetsResponse.PartitionResult r : resp.partitions()) {
                        TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                        if (r.error() == Errors.NONE) {
                            out.put(tp, r);
                            remaining.remove(tp);
                        } else if (!r.error().retriable()) {
                            throw r.error().exception("list offsets for " + tp);
                        } else {
                            missing = true;
                        }
                    }
                } catch (java.util.concurrent.ExecutionException ex) {
                    missing = true;
                }
            }
            if (missing && !remaining.isEmpty()) {
                metadata.refreshSync(config.requestTimeoutMs());
                Thread.sleep(config.retryBackoffMs());
            }
        }
        return out;
    }

    private void sendFetches() {
        Map<BrokerInfo, List<TopicPartition>> byLeader = new HashMap<>();
        boolean needMetadata = false;
        for (Map.Entry<TopicPartition, PartitionState> e : assigned.entrySet()) {
            PartitionState s = e.getValue();
            if (s.position == null || s.fetchInFlight || !s.buffer.isEmpty()) {
                continue;
            }
            BrokerInfo leader = metadata.leaderFor(e.getKey()).orElse(null);
            if (leader == null) {
                needMetadata = true;
                continue;
            }
            if (nodesWithFetchInFlight.contains(leader.id())) {
                continue;
            }
            byLeader.computeIfAbsent(leader, k -> new ArrayList<>()).add(e.getKey());
        }
        if (needMetadata) {
            metadata.refresh();
        }
        for (Map.Entry<BrokerInfo, List<TopicPartition>> e : byLeader.entrySet()) {
            Map<TopicPartition, Long> requested = new LinkedHashMap<>();
            List<FetchRequest.PartitionData> parts = new ArrayList<>();
            for (TopicPartition tp : e.getValue()) {
                PartitionState s = assigned.get(tp);
                s.fetchInFlight = true;
                requested.put(tp, s.position);
                parts.add(new FetchRequest.PartitionData(tp.topic(), tp.partition(), s.position,
                        metadata.leaderEpoch(tp), config.partitionFetchMaxBytes()));
            }
            int nodeId = e.getKey().id();
            nodesWithFetchInFlight.add(nodeId);
            FetchRequest req = new FetchRequest(ApiKey.FETCH, FetchRequest.CONSUMER_REPLICA_ID, -1,
                    config.fetchMaxWaitMs(), config.fetchMinBytes(), config.fetchMaxBytes(), parts);
            network.send(e.getKey().address(), req, FetchResponse::read)
                    .whenComplete((resp, err) -> completed.add(new CompletedFetch(nodeId, requested, resp, err)));
        }
    }

    private void drainCompleted(long waitMs) throws InterruptedException {
        CompletedFetch first = waitMs > 0 ? completed.poll(waitMs, TimeUnit.MILLISECONDS) : completed.poll();
        CompletedFetch c = first;
        while (c != null) {
            handle(c);
            c = completed.poll();
        }
    }

    private void handle(CompletedFetch c) {
        nodesWithFetchInFlight.remove(c.nodeId());
        for (TopicPartition tp : c.requested().keySet()) {
            PartitionState s = assigned.get(tp);
            if (s != null) {
                s.fetchInFlight = false;
            }
        }
        if (c.error() != null) {
            metadata.refresh();
            return;
        }
        for (FetchResponse.PartitionData pd : c.response().partitions()) {
            TopicPartition tp = new TopicPartition(pd.topic(), pd.partition());
            PartitionState s = assigned.get(tp);
            Long requestedAt = c.requested().get(tp);
            if (s == null || s.position == null || !s.position.equals(requestedAt)) {
                continue;
            }
            switch (pd.error()) {
                case NONE -> buffer(tp, s, pd.records() instanceof MemoryRecords m ? m : MemoryRecords.EMPTY);
                case OFFSET_OUT_OF_RANGE -> {
                    s.position = null;
                    s.pendingTimestamp = null;
                    s.checkedCommitted = true;
                }
                default -> metadata.refresh();
            }
        }
    }

    private void buffer(TopicPartition tp, PartitionState s, MemoryRecords records) {
        long position = s.position;
        long end = position;
        for (RecordBatch batch : records.batches()) {
            if (batch.nextOffset() <= position) {
                continue;
            }
            for (Record r : batch.records()) {
                if (r.offset() >= position) {
                    s.buffer.add(new ConsumerRecord(tp.topic(), tp.partition(), r.offset(), r.timestamp(), r.key(),
                            r.value(), r.headers()));
                }
            }
            end = batch.nextOffset();
        }
        if (s.buffer.isEmpty() && end > position) {
            // Every record in the fetched batches was compacted away or before our position.
            s.position = end;
        } else {
            s.bufferEnd = end;
        }
    }

    private List<ConsumerRecord> drainBuffers() {
        List<ConsumerRecord> out = new ArrayList<>();
        int max = config.maxPollRecords();
        for (Iterator<Map.Entry<TopicPartition, PartitionState>> it = assigned.entrySet().iterator();
                it.hasNext() && out.size() < max; ) {
            PartitionState s = it.next().getValue();
            while (!s.buffer.isEmpty() && out.size() < max) {
                ConsumerRecord r = s.buffer.poll();
                out.add(r);
                s.position = r.offset() + 1;
                clock.observe(r.timestamp());
            }
            if (s.buffer.isEmpty() && s.bufferEnd > 0) {
                s.position = Math.max(s.position, s.bufferEnd);
                s.bufferEnd = -1;
            }
        }
        return out;
    }

    /** Commits the current position of every assigned partition. */
    public void commitSync() throws InterruptedException, TimeoutException {
        Map<TopicPartition, Long> positions = new HashMap<>();
        for (Map.Entry<TopicPartition, PartitionState> e : assigned.entrySet()) {
            if (e.getValue().position != null) {
                positions.put(e.getKey(), e.getValue().position);
            }
        }
        commitSync(positions);
    }

    public void commitSync(Map<TopicPartition, Long> positions) throws InterruptedException, TimeoutException {
        if (config.groupId() == null) {
            throw new IllegalStateException("group.id is required to commit offsets");
        }
        offsets.commit(config.groupId(), positions);
    }

    public OptionalLong committed(TopicPartition tp) throws InterruptedException, TimeoutException {
        if (config.groupId() == null) {
            throw new IllegalStateException("group.id is required to read committed offsets");
        }
        return offsets.fetch(config.groupId(), tp);
    }

    @Override
    public void close() {
        network.close();
    }
}
