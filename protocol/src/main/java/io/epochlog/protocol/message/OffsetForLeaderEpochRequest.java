package io.epochlog.protocol.message;

import io.epochlog.protocol.ApiKey;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.RequestMessage;
import java.util.List;

/**
 * Follower to new leader: where does {@code leaderEpoch} end in your log? The follower truncates
 * anything it has past that point before fetching.
 *
 * <p>Thread-safety: immutable.
 */
public record OffsetForLeaderEpochRequest(int replicaId, List<PartitionData> partitions) implements RequestMessage {

    /**
     * One partition's query.
     *
     * <p>Thread-safety: immutable.
     */
    public record PartitionData(String topic, int partition, int currentLeaderEpoch, int leaderEpoch) {}

    @Override
    public ApiKey apiKey() {
        return ApiKey.OFFSET_FOR_LEADER_EPOCH;
    }

    @Override
    public void write(ProtocolWriter w) {
        w.putInt(replicaId);
        w.putArray(partitions, (out, p) -> out.putString(p.topic).putInt(p.partition).putInt(p.currentLeaderEpoch)
                .putInt(p.leaderEpoch));
    }

    public static OffsetForLeaderEpochRequest read(ProtocolReader r) {
        return new OffsetForLeaderEpochRequest(r.getInt(), r.getArray(in -> new PartitionData(in.getString(),
                in.getInt(), in.getInt(), in.getInt())));
    }
}
