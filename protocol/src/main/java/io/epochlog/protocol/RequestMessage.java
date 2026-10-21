package io.epochlog.protocol;

/**
 * A request body.
 *
 * <p>Thread-safety: implementations are immutable records.
 */
public interface RequestMessage {
    ApiKey apiKey();

    void write(ProtocolWriter w);

    /** False for fire-and-forget requests such as produce with {@code acks=0}. */
    default boolean expectsResponse() {
        return true;
    }
}
