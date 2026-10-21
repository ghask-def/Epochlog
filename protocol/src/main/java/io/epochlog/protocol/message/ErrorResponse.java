package io.epochlog.protocol.message;

import io.epochlog.protocol.Errors;
import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import io.epochlog.protocol.ResponseMessage;

/**
 * A response that is just an error code and optional message. Used by CreateTopic.
 *
 * <p>Thread-safety: immutable.
 */
public record ErrorResponse(Errors error, String message) implements ResponseMessage {

    @Override
    public void write(ProtocolWriter w) {
        w.putShort(error.code).putString(message);
    }

    public static ErrorResponse read(ProtocolReader r) {
        return new ErrorResponse(Errors.forCode(r.getShort()), r.getString());
    }
}
