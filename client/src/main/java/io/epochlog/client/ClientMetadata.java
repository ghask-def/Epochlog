package io.epochlog.client;

import io.epochlog.common.TopicPartition;
import io.epochlog.network.NetworkClient;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.message.BrokerInfo;
import io.epochlog.protocol.message.MetadataRequest;
import io.epochlog.protocol.message.MetadataResponse;
import io.epochlog.protocol.message.PartitionState;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client-side view of brokers and partition leaders, refreshed on demand.
 *
 * <p>Thread-safety: thread-safe. The snapshot is immutable and swapped through a volatile field;
 * concurrent refresh calls share one in-flight request.
 */
final class ClientMetadata {
    private static final Logger LOG = LoggerFactory.getLogger(ClientMetadata.class);

    record Snapshot(Map<Integer, BrokerInfo> brokers, Map<TopicPartition, PartitionState> partitions,
            Map<String, Integer> partitionCounts) {}

    private final List<InetSocketAddress> bootstrap;
    private final NetworkClient client;
    private volatile Snapshot snapshot = new Snapshot(Map.of(), Map.of(), Map.of());
    private CompletableFuture<Void> inFlight;

    ClientMetadata(List<InetSocketAddress> bootstrap, NetworkClient client) {
        this.bootstrap = List.copyOf(bootstrap);
        this.client = client;
    }

    Snapshot snapshot() {
        return snapshot;
    }

    /** Starts a refresh unless one is already running. */
    synchronized CompletableFuture<Void> refresh() {
        if (inFlight != null && !inFlight.isDone()) {
            return inFlight;
        }
        CompletableFuture<Void> f = new CompletableFuture<>();
        inFlight = f;
        List<InetSocketAddress> candidates = new ArrayList<>();
        for (BrokerInfo b : snapshot.brokers().values()) {
            candidates.add(b.address());
        }
        candidates.addAll(bootstrap);
        tryRefresh(candidates, ThreadLocalRandom.current().nextInt(candidates.size()), 0, f);
        return f;
    }

    private void tryRefresh(List<InetSocketAddress> candidates, int start, int attempt, CompletableFuture<Void> f) {
        if (attempt >= candidates.size()) {
            f.completeExceptionally(new IllegalStateException("no broker reachable for metadata"));
            return;
        }
        InetSocketAddress target = candidates.get((start + attempt) % candidates.size());
        client.send(target, new MetadataRequest(List.of()), MetadataResponse::read).whenComplete((resp, err) -> {
            if (err != null) {
                LOG.debug("metadata request to {} failed: {}", target, err.toString());
                tryRefresh(candidates, start, attempt + 1, f);
                return;
            }
            update(resp);
            f.complete(null);
        });
    }

    private void update(MetadataResponse resp) {
        Map<Integer, BrokerInfo> brokers = new HashMap<>();
        for (BrokerInfo b : resp.brokers()) {
            brokers.put(b.id(), b);
        }
        Map<TopicPartition, PartitionState> parts = new HashMap<>();
        Map<String, Integer> counts = new HashMap<>();
        for (MetadataResponse.TopicMetadata t : resp.topics()) {
            if (t.error() != Errors.NONE) {
                continue;
            }
            counts.put(t.name(), t.partitions().size());
            for (PartitionState ps : t.partitions()) {
                parts.put(new TopicPartition(ps.topic(), ps.partition()), ps);
            }
        }
        snapshot = new Snapshot(Map.copyOf(brokers), Map.copyOf(parts), Map.copyOf(counts));
    }

    void refreshSync(long timeoutMs) {
        try {
            refresh().get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOG.debug("metadata refresh failed", e);
        }
    }

    int partitionCount(String topic) {
        return snapshot.partitionCounts().getOrDefault(topic, -1);
    }

    Optional<BrokerInfo> leaderFor(TopicPartition tp) {
        Snapshot s = snapshot;
        PartitionState ps = s.partitions().get(tp);
        if (ps == null || ps.leader() < 0) {
            return Optional.empty();
        }
        return Optional.ofNullable(s.brokers().get(ps.leader()));
    }

    int leaderEpoch(TopicPartition tp) {
        PartitionState ps = snapshot.partitions().get(tp);
        return ps == null ? -1 : ps.leaderEpoch();
    }

    InetSocketAddress anyBroker() {
        List<BrokerInfo> known = new ArrayList<>(snapshot.brokers().values());
        if (!known.isEmpty()) {
            return known.get(ThreadLocalRandom.current().nextInt(known.size())).address();
        }
        return bootstrap.get(ThreadLocalRandom.current().nextInt(bootstrap.size()));
    }

    /** Blocks until the topic is known and every partition has a leader. */
    int awaitTopic(String topic, long timeoutMs) throws TimeoutException, InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            int n = partitionCount(topic);
            if (n > 0) {
                boolean allLed = true;
                for (int i = 0; i < n; i++) {
                    allLed &= leaderFor(new TopicPartition(topic, i)).isPresent();
                }
                if (allLed) {
                    return n;
                }
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                throw new TimeoutException("topic " + topic + " not available after " + timeoutMs + " ms");
            }
            refreshSync(Math.min(remaining, 5_000));
            if (partitionCount(topic) <= 0) {
                Thread.sleep(Math.min(100, Math.max(1, remaining)));
            }
        }
    }
}
