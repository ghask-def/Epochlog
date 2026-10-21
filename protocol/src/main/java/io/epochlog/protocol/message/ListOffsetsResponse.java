package io.epochlog.protocol.message;

import io.epochlog.protocol.Errors;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.ResponseMessage;
import java.util.List;

/**
 * Offset lookup results. An offset of -1 means no record at or after the timestamp is committed.
 *
 * <p>Thread-safety: immutable.
 */
public record ListOffsetsResponse(List<PartitionResult> partitions) implements ResponseMessage {

    /**
     * One lookup result.
     *
     * <p>Thread-safety: immutable.
     */
    public record PartitionResult(String topic, int partition, Errors error, long timestamp, long offset) {}

    @Override
    public void write(ProtocolWriter w) {
        w.putArray(partitions, (out, p) -> out.putString(p.topic).putInt(p.partition).putShort(p.error.code)
                .putLong(p.timestamp).putLong(p.offset));
    }

    public static ListOffsetsResponse read(ProtocolReader r) {
        return new ListOffsetsResponse(r.getArray(in -> new PartitionResult(in.getString(), in.getInt(),
                Errors.forCode(in.getShort()), in.getLong(), in.getLong())));
    }
}
