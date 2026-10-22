package io.epochlog.broker;

import io.epochlog.common.Record;
import io.epochlog.common.RecordBatch;
import io.epochlog.common.RecordBatchBuilder;
import io.epochlog.common.TopicPartition;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.MemoryRecords;
import io.epochlog.protocol.OffsetsTopic;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.message.OffsetCommitRequest;
import io.epochlog.protocol.message.OffsetFetchRequest;
import io.epochlog.protocol.message.OffsetFetchResponse;
import io.epochlog.protocol.message.PartitionErrorsResponse;
import io.epochlog.protocol.message.ProduceRequest;
import io.epochlog.storage.FileSlice;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores consumer offsets in the compacted {@code __consumer_offsets} topic. A group belongs to
 * one partition of that topic; the partition's leader serves commits and fetches for it, keeping
 * a cache rebuilt from the log whenever it takes over leadership.
 *
 * <p>Thread-safety: thread-safe.
 */
final class OffsetCoordinator {
    private static final Logger LOG = LoggerFactory.getLogger(OffsetCoordinator.class);

    /** Asks for the offsets topic to be created; implemented differently in standalone and cluster mode. */
    interface TopicCreator {
        void ensureOffsetsTopic();
    }

    private record Loaded(int leaderEpoch, Map<String, Map<TopicPartition, Long>> groups) {}

    private final ReplicaManager replicaManager;
    private final MetadataCache metadata;
    private final TopicCreator topicCreator;
    private final int requestTimeoutMs;
    private final ConcurrentHashMap<Integer, Loaded> caches = new ConcurrentHashMap<>();

    OffsetCoordinator(ReplicaManager replicaManager, MetadataCache metadata, TopicCreator topicCreator,
            int requestTimeoutMs) {
        this.replicaManager = replicaManager;
        this.metadata = metadata;
        this.topicCreator = topicCreator;
        this.requestTimeoutMs = requestTimeoutMs;
    }

    private Partition coordinatorPartition(String group) {
        int n = metadata.partitionCount(OffsetsTopic.NAME);
        if (n == 0) {
            topicCreator.ensureOffsetsTopic();
            return null;
        }
        TopicPartition tp = new TopicPartition(OffsetsTopic.NAME, OffsetsTopic.partitionFor(group, n));
        Partition p = replicaManager.partition(tp).orElse(null);
        return p != null && p.isLeader() ? p : null;
    }

    void commit(OffsetCommitRequest req, Consumer<PartitionErrorsResponse> respond) {
        Partition p = coordinatorPartition(req.group());
        if (p == null) {
            respond.accept(allFailed(req, Errors.COORDINATOR_NOT_AVAILABLE));
            return;
        }
        Loaded cache;
        try {
            cache = ensureLoaded(p);
        } catch (IOException e) {
            respond.accept(allFailed(req, Errors.COORDINATOR_NOT_AVAILABLE));
            return;
        }
        RecordBatchBuilder b = new RecordBatchBuilder();
        for (OffsetCommitRequest.PartitionData pd : req.partitions()) {
            b.append(key(req.group(), pd.topic(), pd.partition()), value(pd.offset()));
        }
        TopicPartition tp = p.topicPartition();
        ProduceRequest produce = new ProduceRequest((short) -1, requestTimeoutMs,
                List.of(new ProduceRequest.PartitionData(tp.topic(), tp.partition(), singleBatch(b.build()))));
        replicaManager.handleProduce(produce, resp -> {
            Errors e = resp.partitions().get(0).error();
            if (e == Errors.NONE) {
                Map<TopicPartition, Long> g = cache.groups().computeIfAbsent(req.group(), k -> new ConcurrentHashMap<>());
                for (OffsetCommitRequest.PartitionData pd : req.partitions()) {
                    g.put(new TopicPartition(pd.topic(), pd.partition()), pd.offset());
                }
            } else if (e == Errors.NOT_LEADER_OR_FOLLOWER) {
                e = Errors.COORDINATOR_NOT_AVAILABLE;
            }
            respond.accept(allFailed(req, e));
        });
    }

    private static MemoryRecords singleBatch(RecordBatch batch) {
        return MemoryRecords.of(List.of(batch));
    }

    OffsetFetchResponse fetch(OffsetFetchRequest req) {
        Partition p = coordinatorPartition(req.group());
        if (p == null) {
            return new OffsetFetchResponse(Errors.COORDINATOR_NOT_AVAILABLE, List.of());
        }
        Loaded cache;
        try {
            cache = ensureLoaded(p);
        } catch (IOException e) {
            return new OffsetFetchResponse(Errors.COORDINATOR_NOT_AVAILABLE, List.of());
        }
        Map<TopicPartition, Long> g = cache.groups().getOrDefault(req.group(), Map.of());
        List<OffsetFetchResponse.PartitionResult> out = new ArrayList<>();
        for (OffsetFetchRequest.PartitionData pd : req.partitions()) {
            Long off = g.get(new TopicPartition(pd.topic(), pd.partition()));
            out.add(new OffsetFetchResponse.PartitionResult(pd.topic(), pd.partition(), Errors.NONE,
                    off == null ? -1 : off));
        }
        return new OffsetFetchResponse(Errors.NONE, out);
    }

    /** Rebuilds the cache from the log the first time we're asked after becoming leader. */
    private Loaded ensureLoaded(Partition p) throws IOException {
        int partition = p.topicPartition().partition();
        int epoch = p.leaderEpoch();
        Loaded current = caches.get(partition);
        if (current != null && current.leaderEpoch() == epoch) {
            return current;
        }
        synchronized (this) {
            current = caches.get(partition);
            if (current != null && current.leaderEpoch() == epoch) {
                return current;
            }
            Map<String, Map<TopicPartition, Long>> groups = new ConcurrentHashMap<>();
            long offset = p.log().logStartOffset();
            long end = p.highWatermark();
            int records = 0;
            while (offset < end) {
                FileSlice slice = p.readCommitted(offset, 1 << 20);
                if (slice == null) {
                    break;
                }
                for (RecordBatch batch : slice.batches()) {
                    for (Record r : batch.records()) {
                        if (r.offset() < offset || r.key() == null || r.value() == null) {
                            continue;
                        }
                        ProtocolReader k = new ProtocolReader(ByteBuffer.wrap(r.key()));
                        String group = k.getString();
                        TopicPartition tp = new TopicPartition(k.getString(), k.getInt());
                        groups.computeIfAbsent(group, x -> new ConcurrentHashMap<>())
                                .put(tp, ByteBuffer.wrap(r.value()).getLong());
                        records++;
                    }
                    offset = batch.nextOffset();
                }
            }
            Loaded loaded = new Loaded(epoch, groups);
            caches.put(partition, loaded);
            LOG.info("loaded {} offset commits for {} groups from {}", records, groups.size(), p.topicPartition());
            return loaded;
        }
    }

    private static byte[] key(String group, String topic, int partition) {
        ByteBuffer b = new ProtocolWriter().putString(group).putString(topic).putInt(partition).toByteBuffer();
        byte[] out = new byte[b.remaining()];
        b.get(out);
        return out;
    }

    private static byte[] value(long offset) {
        return ByteBuffer.allocate(8).putLong(0, offset).array();
    }

    private static PartitionErrorsResponse allFailed(OffsetCommitRequest req, Errors e) {
        List<PartitionErrorsResponse.PartitionError> errs = new ArrayList<>();
        for (OffsetCommitRequest.PartitionData pd : req.partitions()) {
            errs.add(new PartitionErrorsResponse.PartitionError(pd.topic(), pd.partition(), e));
        }
        return new PartitionErrorsResponse(e, errs);
    }

    static Map<String, String> topicConfigs() {
        Map<String, String> c = new HashMap<>();
        c.put("cleanup.policy", "compact");
        c.put("segment.bytes", Integer.toString(64 << 20));
        return c;
    }
}
