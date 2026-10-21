package io.epochlog.protocol.message;

import io.epochlog.protocol.ApiKey;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.RequestMessage;
import java.util.List;

/**
 * Reads from partition leaders. Used for both consumer fetches ({@link ApiKey#FETCH}, bounded
 * by the high watermark) and follower replication ({@link ApiKey#REPLICA_FETCH}, bounded by the
 * log end offset). {@code replicaId} is the follower's broker id, or -1 for consumers.
 *
 * <p>Thread-safety: immutable.
 */
public record FetchRequest(ApiKey apiKey, int replicaId, long brokerEpoch, int maxWaitMs, int minBytes, int maxBytes,
        List<PartitionData> partitions) implements RequestMessage {

    public static final int CONSUMER_REPLICA_ID = -1;

    /**
     * Where to read one partition from.
     *
     * <p>Thread-safety: immutable.
     *
     * @param currentLeaderEpoch the leader epoch the fetcher believes is current, or -1 to skip
     *     the check
     */
    public record PartitionData(String topic, int partition, long fetchOffset, int currentLeaderEpoch, int maxBytes) {}

    @Override
    public void write(ProtocolWriter w) {
        w.putInt(replicaId).putLong(brokerEpoch).putInt(maxWaitMs).putInt(minBytes).putInt(maxBytes);
        w.putArray(partitions, (out, p) -> out.putString(p.topic).putInt(p.partition).putLong(p.fetchOffset)
                .putInt(p.currentLeaderEpoch).putInt(p.maxBytes));
    }

    public static FetchRequest read(ApiKey key, ProtocolReader r) {
        int replicaId = r.getInt();
        long brokerEpoch = r.getLong();
        int maxWait = r.getInt();
        int minBytes = r.getInt();
        int maxBytes = r.getInt();
        List<PartitionData> parts = r.getArray(in -> new PartitionData(in.getString(), in.getInt(), in.getLong(),
                in.getInt(), in.getInt()));
        return new FetchRequest(key, replicaId, brokerEpoch, maxWait, minBytes, maxBytes, parts);
    }
}
