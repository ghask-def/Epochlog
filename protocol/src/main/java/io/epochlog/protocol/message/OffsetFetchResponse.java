package io.epochlog.protocol.message;

import io.epochlog.protocol.Errors;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.ResponseMessage;
import java.util.List;

/**
 * Committed positions; -1 where nothing was committed.
 *
 * <p>Thread-safety: immutable.
 */
public record OffsetFetchResponse(Errors error, List<PartitionResult> partitions) implements ResponseMessage {

    /**
     * One partition's committed offset.
     *
     * <p>Thread-safety: immutable.
     */
    public record PartitionResult(String topic, int partition, Errors error, long offset) {}

    @Override
    public void write(ProtocolWriter w) {
        w.putShort(error.code);
        w.putArray(partitions, (out, p) -> out.putString(p.topic).putInt(p.partition).putShort(p.error.code)
                .putLong(p.offset));
    }

    public static OffsetFetchResponse read(ProtocolReader r) {
        Errors e = Errors.forCode(r.getShort());
        return new OffsetFetchResponse(e, r.getArray(in -> new PartitionResult(in.getString(), in.getInt(),
                Errors.forCode(in.getShort()), in.getLong())));
    }
}
