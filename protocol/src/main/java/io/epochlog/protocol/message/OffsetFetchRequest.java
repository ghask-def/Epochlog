package io.epochlog.protocol.message;

import io.epochlog.protocol.ApiKey;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.RequestMessage;
import java.util.List;

/**
 * Reads committed positions for a group.
 *
 * <p>Thread-safety: immutable.
 */
public record OffsetFetchRequest(String group, List<PartitionData> partitions) implements RequestMessage {

    /**
     * One partition to look up.
     *
     * <p>Thread-safety: immutable.
     */
    public record PartitionData(String topic, int partition) {}

    @Override
    public ApiKey apiKey() {
        return ApiKey.OFFSET_FETCH;
    }

    @Override
    public void write(ProtocolWriter w) {
        w.putString(group);
        w.putArray(partitions, (out, p) -> out.putString(p.topic).putInt(p.partition));
    }

    public static OffsetFetchRequest read(ProtocolReader r) {
        return new OffsetFetchRequest(r.getString(), r.getArray(in -> new PartitionData(in.getString(), in.getInt())));
    }
}
