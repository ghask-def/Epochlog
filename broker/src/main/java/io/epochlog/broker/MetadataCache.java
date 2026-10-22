package io.epochlog.broker;

import io.epochlog.common.TopicPartition;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.message.BrokerInfo;
import io.epochlog.protocol.message.MetadataResponse;
import io.epochlog.protocol.message.PartitionState;
import io.epochlog.protocol.message.TopicConfig;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * The broker's copy of cluster metadata, used to answer Metadata requests and find leaders.
 *
 * <p>Thread-safety: thread-safe. Readers see an immutable snapshot through a volatile field;
 * updates replace the snapshot wholesale.
 */
public final class MetadataCache {

    /**
     * Point-in-time metadata.
     *
     * <p>Thread-safety: immutable.
     */
    public record Snapshot(long version, int controllerId, Map<Integer, BrokerInfo> brokers,
            Map<TopicPartition, PartitionState> partitions, Map<String, Map<String, String>> topicConfigs) {}

    private volatile Snapshot snapshot = new Snapshot(-1, -1, Map.of(), Map.of(), Map.of());

    public Snapshot snapshot() {
        return snapshot;
    }

    public long version() {
        return snapshot.version();
    }

    /** Applies a controller snapshot. Older versions are ignored. Returns false if ignored. */
    public synchronized boolean update(long version, int controllerId, List<BrokerInfo> brokers,
            List<PartitionState> partitions, List<TopicConfig> configs) {
        if (version <= snapshot.version()) {
            return false;
        }
        Map<Integer, BrokerInfo> b = new HashMap<>();
        for (BrokerInfo info : brokers) {
            b.put(info.id(), info);
        }
        Map<TopicPartition, PartitionState> p = new HashMap<>();
        for (PartitionState ps : partitions) {
            p.put(new TopicPartition(ps.topic(), ps.partition()), ps);
        }
        Map<String, Map<String, String>> c = new HashMap<>();
        for (TopicConfig tc : configs) {
            c.put(tc.topic(), tc.configs());
        }
        snapshot = new Snapshot(version, controllerId, Map.copyOf(b), Map.copyOf(p), Map.copyOf(c));
        return true;
    }

    /** Standalone mode: adds or replaces partitions without a controller. */
    public synchronized void putLocal(BrokerInfo self, List<PartitionState> partitions, String topic,
            Map<String, String> configs) {
        Snapshot s = snapshot;
        Map<TopicPartition, PartitionState> p = new HashMap<>(s.partitions());
        for (PartitionState ps : partitions) {
            p.put(new TopicPartition(ps.topic(), ps.partition()), ps);
        }
        Map<String, Map<String, String>> c = new HashMap<>(s.topicConfigs());
        if (topic != null) {
            c.put(topic, Map.copyOf(configs));
        }
        snapshot = new Snapshot(s.version() + 1, self.id(), Map.of(self.id(), self), Map.copyOf(p), Map.copyOf(c));
    }

    public Optional<PartitionState> partitionState(TopicPartition tp) {
        return Optional.ofNullable(snapshot.partitions().get(tp));
    }

    public int partitionCount(String topic) {
        int n = 0;
        for (TopicPartition tp : snapshot.partitions().keySet()) {
            if (tp.topic().equals(topic)) {
                n++;
            }
        }
        return n;
    }

    public boolean topicExists(String topic) {
        return partitionCount(topic) > 0;
    }

    public Optional<BrokerInfo> broker(int id) {
        return Optional.ofNullable(snapshot.brokers().get(id));
    }

    public MetadataResponse describe(List<String> topics) {
        Snapshot s = snapshot;
        Map<String, List<PartitionState>> byTopic = new TreeMap<>();
        for (PartitionState ps : s.partitions().values()) {
            byTopic.computeIfAbsent(ps.topic(), k -> new ArrayList<>()).add(ps);
        }
        List<String> wanted = topics.isEmpty() ? new ArrayList<>(byTopic.keySet()) : topics;
        List<MetadataResponse.TopicMetadata> out = new ArrayList<>();
        for (String t : wanted) {
            List<PartitionState> parts = byTopic.get(t);
            if (parts == null) {
                out.add(new MetadataResponse.TopicMetadata(t, Errors.UNKNOWN_TOPIC_OR_PARTITION, List.of()));
            } else {
                parts.sort(Comparator.comparingInt(PartitionState::partition));
                out.add(new MetadataResponse.TopicMetadata(t, Errors.NONE, parts));
            }
        }
        List<BrokerInfo> brokers = new ArrayList<>(s.brokers().values());
        brokers.sort(Comparator.comparingInt(BrokerInfo::id));
        return new MetadataResponse(brokers, out);
    }
}
