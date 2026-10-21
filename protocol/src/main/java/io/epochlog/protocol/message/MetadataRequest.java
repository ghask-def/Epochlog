package io.epochlog.protocol.message;

import io.epochlog.protocol.ApiKey;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.RequestMessage;
import java.util.List;

/**
 * Asks for brokers and partition leadership. An empty topic list means all topics.
 *
 * <p>Thread-safety: immutable.
 */
public record MetadataRequest(List<String> topics) implements RequestMessage {

    @Override
    public ApiKey apiKey() {
        return ApiKey.METADATA;
    }

    @Override
    public void write(ProtocolWriter w) {
        w.putArray(topics, ProtocolWriter::putString);
    }

    public static MetadataRequest read(ProtocolReader r) {
        return new MetadataRequest(r.getArray(ProtocolReader::getString));
    }
}
