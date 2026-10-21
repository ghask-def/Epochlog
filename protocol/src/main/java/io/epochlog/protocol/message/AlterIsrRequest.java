package io.epochlog.protocol.message;

import io.epochlog.protocol.ApiKey;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.RequestMessage;
import java.util.List;

/**
 * Leader to controller: proposes a new ISR. The controller applies it only if the leader and
 * partition epochs still match, so a deposed leader can't change anything.
 *
 * <p>Thread-safety: immutable.
 */
public record AlterIsrRequest(int brokerId, long brokerEpoch, List<PartitionData> partitions)
        implements RequestMessage {

    /**
     * One proposed change.
     *
     * <p>Thread-safety: immutable.
     */
    public record PartitionData(String topic, int partition, int leaderEpoch, int partitionEpoch,
            List<Integer> newIsr) {}

    @Override
    public ApiKey apiKey() {
        return ApiKey.ALTER_ISR;
    }

    @Override
    public void write(ProtocolWriter w) {
        w.putInt(brokerId).putLong(brokerEpoch);
        w.putArray(partitions, (out, p) -> out.putString(p.topic).putInt(p.partition).putInt(p.leaderEpoch)
                .putInt(p.partitionEpoch).putIntList(p.newIsr));
    }

    public static AlterIsrRequest read(ProtocolReader r) {
        return new AlterIsrRequest(r.getInt(), r.getLong(), r.getArray(in -> new PartitionData(in.getString(),
                in.getInt(), in.getInt(), in.getInt(), in.getIntList())));
    }
}
