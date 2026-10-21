package io.epochlog.storage;

import io.epochlog.common.EpochlogException;

/**
 * An idempotent producer sent a sequence number that skips ahead of what the log has seen.
 *
 * <p>Thread-safety: immutable once constructed.
 */
public class OutOfOrderSequenceException extends EpochlogException {
    public OutOfOrderSequenceException(String message) {
        super(message);
    }
}
