package io.epochlog.network;

import io.epochlog.common.EpochlogException;

/**
 * A connection failed or was closed before a response arrived.
 *
 * <p>Thread-safety: immutable once constructed.
 */
public class NetworkException extends EpochlogException {
    public NetworkException(String message) {
        super(message);
    }

    public NetworkException(String message, Throwable cause) {
        super(message, cause);
    }
}
