package io.epochlog.client;

import io.epochlog.common.Config;
import io.epochlog.common.CompressionType;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Settings shared by the producer, consumer and admin client, plus the per-client ones. Keys
 * mirror the names you'd expect from Kafka where the meaning is the same.
 *
 * <p>Thread-safety: immutable.
 */
public record ClientConfig(
        List<InetSocketAddress> bootstrap,
        String clientId,
        long requestTimeoutMs,
        short acks,
        long lingerMs,
        int batchSize,
        CompressionType compression,
        int retries,
        long retryBackoffMs,
        long deliveryTimeoutMs,
        boolean idempotence,
        int maxInFlightPerPartition,
        long bufferMemory,
        long maxBlockMs,
        String groupId,
        int maxPollRecords,
        int fetchMaxWaitMs,
        int fetchMinBytes,
        int fetchMaxBytes,
        int partitionFetchMaxBytes,
        String autoOffsetReset) {

    public static ClientConfig from(Map<String, String> props) {
        Config c = new Config(props);
        String acks = c.getString("acks", "all");
        return new ClientConfig(
                parseBootstrap(c.getRequired("bootstrap.servers")),
                c.getString("client.id", "epochlog-client"),
                c.getDurationMs("request.timeout.ms", 30_000),
                acks.equalsIgnoreCase("all") ? (short) -1 : Short.parseShort(acks),
                c.getDurationMs("linger.ms", 5),
                c.getInt("batch.size", 64 * 1024),
                CompressionType.forName(c.getString("compression.type", "none")),
                c.getInt("retries", Integer.MAX_VALUE),
                c.getDurationMs("retry.backoff.ms", 100),
                c.getDurationMs("delivery.timeout.ms", 120_000),
                c.getBoolean("enable.idempotence", true),
                c.getInt("max.in.flight.requests.per.partition", 5),
                c.getLong("buffer.memory", 64L << 20),
                c.getDurationMs("max.block.ms", 60_000),
                c.getString("group.id", null),
                c.getInt("max.poll.records", 1000),
                (int) c.getDurationMs("fetch.max.wait.ms", 500),
                c.getInt("fetch.min.bytes", 1),
                c.getInt("fetch.max.bytes", 50 << 20),
                c.getInt("max.partition.fetch.bytes", 1 << 20),
                c.getString("auto.offset.reset", "earliest"));
    }

    public static List<InetSocketAddress> parseBootstrap(String servers) {
        List<InetSocketAddress> out = new ArrayList<>();
        for (String s : servers.split(",")) {
            String hp = s.trim();
            if (hp.isEmpty()) {
                continue;
            }
            int colon = hp.lastIndexOf(':');
            out.add(InetSocketAddress.createUnresolved(hp.substring(0, colon), Integer.parseInt(hp.substring(colon + 1))));
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("bootstrap.servers is empty");
        }
        return out;
    }
}
