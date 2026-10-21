package io.epochlog.protocol.message;

import io.epochlog.protocol.Errors;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.ResponseMessage;
import java.util.List;

/**
 * Result of an ISR change: the state the controller now holds for each partition.
 *
 * <p>Thread-safety: immutable.
 */
public record AlterIsrResponse(Errors error, List<PartitionResult> partitions) implements ResponseMessage {

    /**
     * One partition's outcome.
     *
     * <p>Thread-safety: immutable.
     */
    public record PartitionResult(String topic, int partition, Errors error, int leaderEpoch, int partitionEpoch,
            List<Integer> isr) {}

    @Override
    public void write(ProtocolWriter w) {
        w.putShort(error.code);
        w.putArray(partitions, (out, p) -> out.putString(p.topic).putInt(p.partition).putShort(p.error.code)
                .putInt(p.leaderEpoch).putInt(p.partitionEpoch).putIntList(p.isr));
    }

    public static AlterIsrResponse read(ProtocolReader r) {
        Errors e = Errors.forCode(r.getShort());
        return new AlterIsrResponse(e, r.getArray(in -> new PartitionResult(in.getString(), in.getInt(),
                Errors.forCode(in.getShort()), in.getInt(), in.getInt(), in.getIntList())));
    }
}
