package io.epochlog.storage;

import io.epochlog.common.EpochlogException;

/**
 * A producer id has no state on this log and the batch does not start at sequence zero.
 *
 * <p>Thread-safety: immutable once constructed.
 */
public class UnknownProducerIdException extends EpochlogException {
    public UnknownProducerIdException(String message) {
        super(message);
    }
}
