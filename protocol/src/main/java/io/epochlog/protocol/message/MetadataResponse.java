package io.epochlog.protocol.message;

import io.epochlog.protocol.Errors;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.ResponseMessage;
import java.util.List;

/**
 * Cluster view: live brokers and, per requested topic, each partition's state.
 *
 * <p>Thread-safety: immutable.
 */
public record MetadataResponse(List<BrokerInfo> brokers, List<TopicMetadata> topics) implements ResponseMessage {

    /**
     * One topic.
     *
     * <p>Thread-safety: immutable.
     */
    public record TopicMetadata(String name, Errors error, List<PartitionState> partitions) {}

    @Override
    public void write(ProtocolWriter w) {
        w.putArray(brokers, (out, b) -> b.write(out));
        w.putArray(topics, (out, t) -> {
            out.putString(t.name).putShort(t.error.code);
            out.putArray(t.partitions, (o, p) -> p.write(o));
        });
    }

    public static MetadataResponse read(ProtocolReader r) {
        List<BrokerInfo> brokers = r.getArray(BrokerInfo::read);
        List<TopicMetadata> topics = r.getArray(in -> new TopicMetadata(in.getString(), Errors.forCode(in.getShort()),
                in.getArray(PartitionState::read)));
        return new MetadataResponse(brokers, topics);
    }
}
