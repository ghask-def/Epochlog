package io.epochlog.protocol.message;

import io.epochlog.protocol.ApiKey;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.RequestMessage;
import java.util.List;

/**
 * Controller to broker: the full cluster state as of {@code metadataVersion}. Each broker acts
 * on the partitions it replicates and caches the rest for metadata requests. Sending the whole
 * snapshot keeps brokers convergent even if an earlier push was lost.
 *
 * <p>Thread-safety: immutable.
 */
public record LeaderAndIsrRequest(int controllerId, long metadataVersion, List<BrokerInfo> liveBrokers,
        List<PartitionState> partitions, List<TopicConfig> topicConfigs) implements RequestMessage {

    @Override
    public ApiKey apiKey() {
        return ApiKey.LEADER_AND_ISR;
    }

    @Override
    public void write(ProtocolWriter w) {
        w.putInt(controllerId).putLong(metadataVersion);
        w.putArray(liveBrokers, (out, b) -> b.write(out));
        w.putArray(partitions, (out, p) -> p.write(out));
        w.putArray(topicConfigs, (out, t) -> t.write(out));
    }

    public static LeaderAndIsrRequest read(ProtocolReader r) {
        return new LeaderAndIsrRequest(r.getInt(), r.getLong(), r.getArray(BrokerInfo::read),
                r.getArray(PartitionState::read), r.getArray(TopicConfig::read));
    }
}
