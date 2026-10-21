package io.epochlog.protocol.message;

import io.epochlog.protocol.Errors;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.Records;
import io.epochlog.protocol.ResponseMessage;
import java.util.List;

/**
 * Fetched batches per partition, with the leader's high watermark and log start offset.
 *
 * <p>Thread-safety: immutable.
 */
public record FetchResponse(List<PartitionData> partitions) implements ResponseMessage {

    /**
     * One partition's data.
     *
     * <p>Thread-safety: immutable.
     */
    public record PartitionData(String topic, int partition, Errors error, long highWatermark, long logStartOffset,
            long logEndOffset, Records records) {}

    @Override
    public void write(ProtocolWriter w) {
        w.putArray(partitions, (out, p) -> out.putString(p.topic).putInt(p.partition).putShort(p.error.code)
                .putLong(p.highWatermark).putLong(p.logStartOffset).putLong(p.logEndOffset).putRecords(p.records));
    }

    public static FetchResponse read(ProtocolReader r) {
        return new FetchResponse(r.getArray(in -> new PartitionData(in.getString(), in.getInt(),
                Errors.forCode(in.getShort()), in.getLong(), in.getLong(), in.getLong(), in.getRecords())));
    }
}
