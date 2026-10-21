package io.epochlog.protocol.message;

import io.epochlog.protocol.Errors;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.ResponseMessage;
import java.util.List;

/**
 * Per-partition produce results. {@code logAppendTime} is the HLC timestamp the leader stamped
 * on the first record.
 *
 * <p>Thread-safety: immutable.
 */
public record ProduceResponse(List<PartitionResult> partitions) implements ResponseMessage {

    /**
     * One partition's outcome.
     *
     * <p>Thread-safety: immutable.
     */
    public record PartitionResult(String topic, int partition, Errors error, long baseOffset, long logAppendTime) {}

    @Override
    public void write(ProtocolWriter w) {
        w.putArray(partitions, (out, p) -> out.putString(p.topic).putInt(p.partition).putShort(p.error.code)
                .putLong(p.baseOffset).putLong(p.logAppendTime));
    }

    public static ProduceResponse read(ProtocolReader r) {
        return new ProduceResponse(r.getArray(in -> new PartitionResult(in.getString(), in.getInt(),
                Errors.forCode(in.getShort()), in.getLong(), in.getLong())));
    }
}
