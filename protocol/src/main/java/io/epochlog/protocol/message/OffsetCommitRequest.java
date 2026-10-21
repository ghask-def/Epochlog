package io.epochlog.protocol.message;

import io.epochlog.protocol.ApiKey;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.RequestMessage;
import java.util.List;

/**
 * Stores consumer positions for a group. Sent to the leader of the group's offsets partition.
 *
 * <p>Thread-safety: immutable.
 */
public record OffsetCommitRequest(String group, List<PartitionData> partitions) implements RequestMessage {

    /**
     * One committed position.
     *
     * <p>Thread-safety: immutable.
     */
    public record PartitionData(String topic, int partition, long offset) {}

    @Override
    public ApiKey apiKey() {
        return ApiKey.OFFSET_COMMIT;
    }

    @Override
    public void write(ProtocolWriter w) {
        w.putString(group);
        w.putArray(partitions, (out, p) -> out.putString(p.topic).putInt(p.partition).putLong(p.offset));
    }

    public static OffsetCommitRequest read(ProtocolReader r) {
        return new OffsetCommitRequest(r.getString(), r.getArray(in -> new PartitionData(in.getString(), in.getInt(),
                in.getLong())));
    }
}
