package io.epochlog.chaos;

import io.epochlog.client.Consumer;
import io.epochlog.client.ConsumerRecord;
import io.epochlog.common.TopicPartition;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Records what producers had acknowledged and what a consumer observed while faults were
 * happening, then checks both against the final, converged log:
 *
 * <ul>
 *   <li>every acknowledged record is in the final log;
 *   <li>every record a consumer read is in the final log at the same offset with the same
 *       value, i.e. consumers never see a record that later disappears.
 * </ul>
 *
 * <p>Thread-safety: thread-safe for recording; {@link #verify} runs after recording stops.
 */
public final class History {
    private static final Logger LOG = LoggerFactory.getLogger(History.class);

    private final Set<String> acked = ConcurrentHashMap.newKeySet();
    private final Map<TopicPartition, Map<Long, String>> observed = new ConcurrentHashMap<>();
    private volatile Thread reader;
    private volatile boolean reading;

    public void acked(String value) {
        acked.add(value);
    }

    public Set<String> ackedValues() {
        return Set.copyOf(acked);
    }

    /** Starts a background consumer that records everything it reads. */
    public void startReading(ChaosCluster cluster, List<TopicPartition> partitions) {
        reading = true;
        reader = Thread.ofPlatform().name("history-reader").daemon().start(() -> {
            try (Consumer c = cluster.consumer("auto.offset.reset", "earliest", "fetch.max.wait.ms", "100")) {
                c.assign(partitions);
                while (reading) {
                    for (ConsumerRecord r : c.poll(Duration.ofMillis(100))) {
                        String prev = observed.computeIfAbsent(r.topicPartition(), k -> new ConcurrentHashMap<>())
                                .putIfAbsent(r.offset(), r.valueString());
                        if (prev != null && !prev.equals(r.valueString())) {
                            throw new AssertionError("offset " + r.offset() + " of " + r.topicPartition()
                                    + " was read as both " + prev + " and " + r.valueString());
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    public void stopReading() throws InterruptedException {
        reading = false;
        if (reader != null) {
            reader.join(10_000);
        }
    }

    public int observedCount() {
        int n = 0;
        for (Map<Long, String> m : observed.values()) {
            n += m.size();
        }
        return n;
    }

    /** Reads every partition to its end and checks the invariants. Returns the final log contents. */
    public Map<TopicPartition, Map<Long, String>> verify(ChaosCluster cluster, List<TopicPartition> partitions)
            throws Exception {
        Map<TopicPartition, Map<Long, String>> finalLog = new TreeMap<>();
        try (Consumer c = cluster.consumer("auto.offset.reset", "earliest", "fetch.max.wait.ms", "50")) {
            for (TopicPartition tp : partitions) {
                c.assign(List.of(tp));
                c.seekToEnd(List.of(tp));
                long end = c.position(tp);
                c.seekToBeginning(List.of(tp));
                Map<Long, String> contents = new TreeMap<>();
                long deadline = System.currentTimeMillis() + 30_000;
                while (c.position(tp) < end && System.currentTimeMillis() < deadline) {
                    for (ConsumerRecord r : c.poll(Duration.ofMillis(100))) {
                        contents.put(r.offset(), r.valueString());
                    }
                }
                finalLog.put(tp, contents);
            }
        }
        Set<String> present = new HashSet<>();
        for (Map<Long, String> m : finalLog.values()) {
            present.addAll(m.values());
        }
        List<String> lost = new ArrayList<>();
        for (String v : acked) {
            if (!present.contains(v)) {
                lost.add(v);
            }
        }
        if (!lost.isEmpty()) {
            throw new AssertionError(lost.size() + " acknowledged records lost, e.g. "
                    + lost.subList(0, Math.min(5, lost.size())));
        }
        for (Map.Entry<TopicPartition, Map<Long, String>> e : observed.entrySet()) {
            Map<Long, String> fin = finalLog.getOrDefault(e.getKey(), Map.of());
            for (Map.Entry<Long, String> seen : e.getValue().entrySet()) {
                String now = fin.get(seen.getKey());
                if (!seen.getValue().equals(now)) {
                    throw new AssertionError("consumer read " + seen.getValue() + " at " + e.getKey() + "@"
                            + seen.getKey() + " but the final log has " + now);
                }
            }
        }
        LOG.info("history ok: {} acked, {} observed by consumer, {} in final log", acked.size(), observedCount(),
                present.size());
        return finalLog;
    }
}
