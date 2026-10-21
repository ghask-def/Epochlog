package io.epochlog.storage;

import io.epochlog.common.EpochlogException;

/**
 * A batch exceeds {@code max.message.bytes}.
 *
 * <p>Thread-safety: immutable once constructed.
 */
public class RecordTooLargeException extends EpochlogException {
    public RecordTooLargeException(String message) {
        super(message);
    }
}
