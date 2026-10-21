package io.epochlog.common;

/**
 * Base unchecked exception for epochlog.
 *
 * <p>Thread-safety: immutable once constructed.
 */
public class EpochlogException extends RuntimeException {
    public EpochlogException(String message) {
        super(message);
    }

    public EpochlogException(String message, Throwable cause) {
        super(message, cause);
    }
}
