package io.epochlog.common;

/**
 * Thrown when a record batch fails a CRC or structural check.
 *
 * <p>Thread-safety: immutable once constructed.
 */
public class CorruptRecordException extends EpochlogException {
    public CorruptRecordException(String message) {
        super(message);
    }

    public CorruptRecordException(String message, Throwable cause) {
        super(message, cause);
    }
}
