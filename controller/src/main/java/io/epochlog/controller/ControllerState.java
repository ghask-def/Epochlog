package io.epochlog.controller;

import io.epochlog.common.AtomicFiles;
import io.epochlog.common.EpochlogException;
import io.epochlog.common.TopicPartition;
import io.epochlog.protocol.message.PartitionState;
import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Everything the controller must remember across restarts: topics, partition assignments with
 * their leader and partition epochs, the broker epoch counter and the metadata version. Saved to
 * a small text file with an atomic rename after every change.
 *
 * <p>Thread-safety: not thread-safe. Owned by {@link Controller}, which synchronizes.
 */
final class ControllerState {
    private static final String FILE = "controller.state";

    final Map<String, Map<String, String>> topicConfigs = new TreeMap<>();
    final Map<TopicPartition, PartitionState> partitions = new TreeMap<>();
    long metadataVersion;
    long brokerEpochCounter;

    private final Path file;

    private ControllerState(Path dir) {
        this.file = dir.resolve(FILE);
    }

    static ControllerState load(Path dir) throws IOException {
        Files.createDirectories(dir);
        ControllerState s = new ControllerState(dir);
        if (!Files.exists(s.file)) {
            return s;
        }
        for (String line : Files.readAllLines(s.file)) {
            if (line.isBlank()) {
                continue;
            }
            String[] f = line.split(" ");
            switch (f[0]) {
                case "version" -> s.metadataVersion = Long.parseLong(f[1]);
                case "broker-epoch" -> s.brokerEpochCounter = Long.parseLong(f[1]);
                case "topic" -> s.topicConfigs.put(decode(f[1]), parseConfigs(f.length > 2 ? f[2] : ""));
                case "partition" -> {
                    PartitionState ps = new PartitionState(decode(f[1]), Integer.parseInt(f[2]), Integer.parseInt(f[3]),
                            Integer.parseInt(f[4]), Integer.parseInt(f[5]), ints(f[6]), ints(f[7]));
                    s.partitions.put(new TopicPartition(ps.topic(), ps.partition()), ps);
                }
                default -> throw new EpochlogException("bad controller state line: " + line);
            }
        }
        return s;
    }

    void save() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("version ").append(metadataVersion).append('\n');
        sb.append("broker-epoch ").append(brokerEpochCounter).append('\n');
        topicConfigs.forEach((t, c) -> sb.append("topic ").append(encode(t)).append(' ').append(formatConfigs(c)).append('\n'));
        for (PartitionState ps : partitions.values()) {
            sb.append("partition ").append(encode(ps.topic())).append(' ').append(ps.partition()).append(' ')
                    .append(ps.leader()).append(' ').append(ps.leaderEpoch()).append(' ').append(ps.partitionEpoch())
                    .append(' ').append(csv(ps.replicas())).append(' ').append(csv(ps.isr())).append('\n');
        }
        AtomicFiles.write(file, sb.toString());
    }

    List<PartitionState> partitionsOf(String topic) {
        List<PartitionState> out = new ArrayList<>();
        for (PartitionState ps : partitions.values()) {
            if (ps.topic().equals(topic)) {
                out.add(ps);
            }
        }
        return out;
    }

    private static String csv(List<Integer> ids) {
        if (ids.isEmpty()) {
            return "-";
        }
        StringBuilder sb = new StringBuilder();
        for (int id : ids) {
            if (!sb.isEmpty()) {
                sb.append(',');
            }
            sb.append(id);
        }
        return sb.toString();
    }

    private static List<Integer> ints(String csv) {
        List<Integer> out = new ArrayList<>();
        if (!csv.equals("-")) {
            for (String s : csv.split(",")) {
                out.add(Integer.parseInt(s));
            }
        }
        return out;
    }

    private static String formatConfigs(Map<String, String> c) {
        if (c.isEmpty()) {
            return "-";
        }
        StringBuilder sb = new StringBuilder();
        c.forEach((k, v) -> {
            if (!sb.isEmpty()) {
                sb.append('&');
            }
            sb.append(encode(k)).append('=').append(encode(v));
        });
        return sb.toString();
    }

    private static Map<String, String> parseConfigs(String s) {
        Map<String, String> m = new LinkedHashMap<>();
        if (s.isEmpty() || s.equals("-")) {
            return m;
        }
        for (String kv : s.split("&")) {
            int eq = kv.indexOf('=');
            m.put(decode(kv.substring(0, eq)), decode(kv.substring(eq + 1)));
        }
        return m;
    }

    private static String encode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }

    private static String decode(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }
}
