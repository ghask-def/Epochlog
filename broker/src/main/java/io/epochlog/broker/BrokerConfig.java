package io.epochlog.broker;

import io.epochlog.common.Config;
import io.epochlog.storage.LogConfig;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Broker settings. Keys prefixed {@code log.} set the default {@link LogConfig} for new topics.
 * Leaving {@code controller} unset runs the broker standalone: it leads every partition itself
 * and creates topics locally.
 *
 * <p>Thread-safety: immutable.
 */
public record BrokerConfig(
        int brokerId,
        String host,
        int port,
        String advertisedHost,
        Path dataDir,
        InetSocketAddress controller,
        int networkThreads,
        boolean virtualHandlerThreads,
        int handlerThreads,
        boolean groupCommit,
        long groupCommitMaxWaitMicros,
        int groupCommitMaxBatch,
        long replicaLagTimeMaxMs,
        int replicaFetchMaxWaitMs,
        int replicaFetchMaxBytes,
        int minInsyncReplicas,
        long heartbeatIntervalMs,
        long sessionTimeoutMs,
        long requestTimeoutMs,
        long retentionCheckIntervalMs,
        int maxRequestBytes,
        int offsetsTopicPartitions,
        long maxClockDriftMs,
        LogConfig logDefaults) {

    public static BrokerConfig from(Config c) {
        Map<String, String> logKeys = new HashMap<>();
        c.asMap().forEach((k, v) -> {
            if (k.startsWith("log.") && !k.equals("log.retention.check.interval.ms")) {
                logKeys.put(k.substring(4), v);
            }
        });
        String controller = c.getString("controller", "").trim();
        String handlers = c.getString("handler.threads", "virtual").trim();
        boolean virtual = handlers.equalsIgnoreCase("virtual");
        return new BrokerConfig(
                c.getInt("broker.id", 0),
                c.getString("host", "0.0.0.0"),
                c.getInt("port", 9092),
                c.getString("advertised.host", "localhost"),
                Path.of(c.getString("data.dir", "data/broker-" + c.getInt("broker.id", 0))),
                controller.isEmpty() ? null : parseAddress(controller),
                c.getInt("network.threads", 3),
                virtual,
                virtual ? 0 : Integer.parseInt(handlers),
                c.getBoolean("group.commit.enabled", true),
                c.getLong("group.commit.max.wait.us", 2000),
                c.getInt("group.commit.max.batch", 1024),
                c.getDurationMs("replica.lag.time.max.ms", 10_000),
                (int) c.getDurationMs("replica.fetch.max.wait.ms", 500),
                c.getInt("replica.fetch.max.bytes", 1 << 20),
                c.getInt("min.insync.replicas", 1),
                c.getDurationMs("heartbeat.interval.ms", 500),
                c.getDurationMs("broker.session.timeout.ms", 6_000),
                c.getDurationMs("request.timeout.ms", 30_000),
                c.getDurationMs("log.retention.check.interval.ms", 300_000),
                c.getInt("max.request.bytes", 100 << 20),
                c.getInt("offsets.topic.partitions", 8),
                c.getDurationMs("hlc.max.drift.ms", 500),
                LogConfig.from(new Config(logKeys)));
    }

    public static InetSocketAddress parseAddress(String hostPort) {
        int colon = hostPort.lastIndexOf(':');
        if (colon < 0) {
            throw new IllegalArgumentException("expected host:port, got " + hostPort);
        }
        return InetSocketAddress.createUnresolved(hostPort.substring(0, colon),
                Integer.parseInt(hostPort.substring(colon + 1)));
    }

    public boolean standalone() {
        return controller == null;
    }
}
