package io.epochlog.protocol.message;

import io.epochlog.protocol.ApiKey;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.RequestMessage;
import java.util.List;

/**
 * Looks up offsets by HLC timestamp, or the earliest/latest offset via the sentinel values.
 *
 * <p>Thread-safety: immutable.
 */
public record ListOffsetsRequest(List<PartitionData> partitions) implements RequestMessage {
    public static final long LATEST = -1L;
    public static final long EARLIEST = -2L;

    /**
     * One lookup.
     *
     * <p>Thread-safety: immutable.
     */
    public record PartitionData(String topic, int partition, long timestamp) {}

    @Override
    public ApiKey apiKey() {
        return ApiKey.LIST_OFFSETS;
    }

    @Override
    public void write(ProtocolWriter w) {
        w.putArray(partitions, (out, p) -> out.putString(p.topic).putInt(p.partition).putLong(p.timestamp));
    }

    public static ListOffsetsRequest read(ProtocolReader r) {
        return new ListOffsetsRequest(r.getArray(in -> new PartitionData(in.getString(), in.getInt(), in.getLong())));
    }
}
