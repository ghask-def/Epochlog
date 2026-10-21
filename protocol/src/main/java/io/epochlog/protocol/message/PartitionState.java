package io.epochlog.protocol.message;

import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import java.util.List;

/**
 * Controller-assigned state of one partition. {@code leaderEpoch} changes when leadership
 * moves; {@code partitionEpoch} changes on any update, including ISR changes, and guards
 * against applying ISR updates out of order.
 *
 * <p>Thread-safety: immutable.
 */
public record PartitionState(String topic, int partition, int leader, int leaderEpoch, int partitionEpoch,
        List<Integer> replicas, List<Integer> isr) {

    public static final int NO_LEADER = -1;

    public PartitionState {
        replicas = List.copyOf(replicas);
        isr = List.copyOf(isr);
    }

    public void write(ProtocolWriter w) {
        w.putString(topic).putInt(partition).putInt(leader).putInt(leaderEpoch).putInt(partitionEpoch)
                .putIntList(replicas).putIntList(isr);
    }

    public static PartitionState read(ProtocolReader r) {
        return new PartitionState(r.getString(), r.getInt(), r.getInt(), r.getInt(), r.getInt(), r.getIntList(),
                r.getIntList());
    }
}
