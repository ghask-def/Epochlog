package io.epochlog.protocol;

/**
 * Header on every request: which API, a correlation id echoed in the response, and the sender.
 *
 * <p>Thread-safety: immutable.
 */
public record RequestHeader(ApiKey apiKey, short version, int correlationId, String clientId) {

    public void write(ProtocolWriter w) {
        w.putShort(apiKey.id).putShort(version).putInt(correlationId).putString(clientId);
    }

    public static RequestHeader read(ProtocolReader r) {
        return new RequestHeader(ApiKey.forId(r.getShort()), r.getShort(), r.getInt(), r.getString());
    }
}
