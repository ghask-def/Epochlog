package io.epochlog.protocol.message;

import io.epochlog.protocol.Errors;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.ResponseMessage;

/**
 * The assigned producer id.
 *
 * <p>Thread-safety: immutable.
 */
public record InitProducerIdResponse(Errors error, long producerId) implements ResponseMessage {

    @Override
    public void write(ProtocolWriter w) {
        w.putShort(error.code).putLong(producerId);
    }

    public static InitProducerIdResponse read(ProtocolReader r) {
        return new InitProducerIdResponse(Errors.forCode(r.getShort()), r.getLong());
    }
}
