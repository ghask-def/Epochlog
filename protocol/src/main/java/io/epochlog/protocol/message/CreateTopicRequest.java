package io.epochlog.protocol.message;

import io.epochlog.protocol.ApiKey;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.RequestMessage;
import java.util.Map;

/**
 * Creates a topic. Brokers forward this to the controller.
 *
 * <p>Thread-safety: immutable.
 */
public record CreateTopicRequest(String name, int partitions, short replicationFactor, Map<String, String> configs)
        implements RequestMessage {

    public CreateTopicRequest {
        configs = Map.copyOf(configs);
    }

    @Override
    public ApiKey apiKey() {
        return ApiKey.CREATE_TOPIC;
    }

    @Override
    public void write(ProtocolWriter w) {
        w.putString(name).putInt(partitions).putShort(replicationFactor);
        TopicConfig.writeMap(w, configs);
    }

    public static CreateTopicRequest read(ProtocolReader r) {
        return new CreateTopicRequest(r.getString(), r.getInt(), r.getShort(), TopicConfig.readMap(r));
    }
}
