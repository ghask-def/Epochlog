package io.epochlog.protocol.message;

import io.epochlog.protocol.Errors;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.ResponseMessage;
import java.util.List;

/**
 * A top-level error plus per-partition errors. Used by LeaderAndIsr and OffsetCommit.
 *
 * <p>Thread-safety: immutable.
 */
public record PartitionErrorsResponse(Errors error, List<PartitionError> partitions) implements ResponseMessage {

    /**
     * One partition's error.
     *
     * <p>Thread-safety: immutable.
     */
    public record PartitionError(String topic, int partition, Errors error) {}

    @Override
    public void write(ProtocolWriter w) {
        w.putShort(error.code);
        w.putArray(partitions, (out, p) -> out.putString(p.topic).putInt(p.partition).putShort(p.error.code));
    }

    public static PartitionErrorsResponse read(ProtocolReader r) {
        Errors e = Errors.forCode(r.getShort());
        return new PartitionErrorsResponse(e, r.getArray(in -> new PartitionError(in.getString(), in.getInt(),
                Errors.forCode(in.getShort()))));
    }
}
