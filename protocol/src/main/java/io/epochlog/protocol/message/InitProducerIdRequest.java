package io.epochlog.protocol.message;

import io.epochlog.protocol.ApiKey;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.RequestMessage;

/**
 * Asks a broker for a fresh producer id for idempotent produce.
 *
 * <p>Thread-safety: immutable.
 */
public record InitProducerIdRequest() implements RequestMessage {

    @Override
    public ApiKey apiKey() {
        return ApiKey.INIT_PRODUCER_ID;
    }

    @Override
    public void write(ProtocolWriter w) {}

    public static InitProducerIdRequest read(ProtocolReader r) {
        return new InitProducerIdRequest();
    }
}
