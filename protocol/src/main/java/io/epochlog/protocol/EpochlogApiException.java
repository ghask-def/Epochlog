package io.epochlog.protocol;

import io.epochlog.common.EpochlogException;

/**
 * An error returned by a broker or controller, carrying its {@link Errors} code.
 *
 * <p>Thread-safety: immutable once constructed.
 */
public class EpochlogApiException extends EpochlogException {
    private final Errors error;

    public EpochlogApiException(Errors error, String message) {
        super(error + ": " + message);
        this.error = error;
    }

    public Errors error() {
        return error;
    }

    public boolean retriable() {
        return error.retriable();
    }
}
