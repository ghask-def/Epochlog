package io.epochlog.storage;

import io.epochlog.common.EpochlogException;

/**
 * A read asked for an offset outside {@code [logStartOffset, logEndOffset]}.
 *
 * <p>Thread-safety: immutable once constructed.
 */
public class OffsetOutOfRangeException extends EpochlogException {
    public OffsetOutOfRangeException(String message) {
        super(message);
    }
}
