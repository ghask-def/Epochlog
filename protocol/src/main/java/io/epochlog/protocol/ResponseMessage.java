package io.epochlog.protocol;

/**
 * A response body.
 *
 * <p>Thread-safety: implementations are immutable records.
 */
public interface ResponseMessage {
    void write(ProtocolWriter w);
}
