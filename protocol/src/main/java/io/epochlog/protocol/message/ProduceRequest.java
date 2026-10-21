package io.epochlog.protocol.message;

import io.epochlog.protocol.ApiKey;
import io.epochlog.protocol.MemoryRecords;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.RequestMessage;
import java.util.List;

/**
 * Appends batches to partition leaders. {@code acks} is 0, 1 or -1 (all in-sync replicas).
 *
 * <p>Thread-safety: immutable.
 */
public record ProduceRequest(short acks, int timeoutMs, List<PartitionData> partitions) implements RequestMessage {

    /**
     * Batches for one partition.
     *
     * <p>Thread-safety: immutable.
     */
    public record PartitionData(String topic, int partition, MemoryRecords records) {}

    @Override
    public ApiKey apiKey() {
        return ApiKey.PRODUCE;
    }

    @Override
    public boolean expectsResponse() {
        return acks != 0;
    }

    @Override
    public void write(ProtocolWriter w) {
        w.putShort(acks).putInt(timeoutMs);
        w.putArray(partitions, (out, p) -> out.putString(p.topic).putInt(p.partition).putRecords(p.records));
    }

    public static ProduceRequest read(ProtocolReader r) {
        short acks = r.getShort();
        int timeout = r.getInt();
        List<PartitionData> parts = r.getArray(in -> new PartitionData(in.getString(), in.getInt(), in.getRecords()));
        return new ProduceRequest(acks, timeout, parts);
    }
}
