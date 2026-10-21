package io.epochlog.protocol.message;

import io.epochlog.protocol.Errors;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.ResponseMessage;
import java.util.List;

/**
 * End offsets per epoch. {@code leaderEpoch} may be lower than asked for if the leader never saw
 * that epoch; -1 values mean the leader has no information.
 *
 * <p>Thread-safety: immutable.
 */
public record OffsetForLeaderEpochResponse(List<PartitionResult> partitions) implements ResponseMessage {

    /**
     * One partition's answer.
     *
     * <p>Thread-safety: immutable.
     */
    public record PartitionResult(String topic, int partition, Errors error, int leaderEpoch, long endOffset) {}

    @Override
    public void write(ProtocolWriter w) {
        w.putArray(partitions, (out, p) -> out.putString(p.topic).putInt(p.partition).putShort(p.error.code)
                .putInt(p.leaderEpoch).putLong(p.endOffset));
    }

    public static OffsetForLeaderEpochResponse read(ProtocolReader r) {
        return new OffsetForLeaderEpochResponse(r.getArray(in -> new PartitionResult(in.getString(), in.getInt(),
                Errors.forCode(in.getShort()), in.getInt(), in.getLong())));
    }
}
