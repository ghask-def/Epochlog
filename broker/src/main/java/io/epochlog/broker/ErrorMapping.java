package io.epochlog.broker;

import io.epochlog.common.CorruptRecordException;
import io.epochlog.protocol.EpochlogApiException;
import io.epochlog.protocol.Errors;
import io.epochlog.storage.OffsetOutOfRangeException;
import io.epochlog.storage.OutOfOrderSequenceException;
import io.epochlog.storage.RecordTooLargeException;
import io.epochlog.storage.UnknownProducerIdException;
import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps exceptions thrown inside the broker to wire error codes.
 *
 * <p>Thread-safety: thread-safe (stateless).
 */
final class ErrorMapping {
    private static final Logger LOG = LoggerFactory.getLogger(ErrorMapping.class);

    private ErrorMapping() {}

    static Errors of(Throwable t) {
        while ((t instanceof CompletionException || t instanceof ExecutionException) && t.getCause() != null) {
            t = t.getCause();
        }
        if (t instanceof EpochlogApiException api) {
            return api.error();
        }
        if (t instanceof OffsetOutOfRangeException) {
            return Errors.OFFSET_OUT_OF_RANGE;
        }
        if (t instanceof CorruptRecordException) {
            return Errors.CORRUPT_MESSAGE;
        }
        if (t instanceof RecordTooLargeException) {
            return Errors.MESSAGE_TOO_LARGE;
        }
        if (t instanceof OutOfOrderSequenceException) {
            return Errors.OUT_OF_ORDER_SEQUENCE_NUMBER;
        }
        if (t instanceof UnknownProducerIdException) {
            return Errors.UNKNOWN_PRODUCER_ID;
        }
        if (t instanceof IOException) {
            LOG.error("storage error", t);
            return Errors.STORAGE_ERROR;
        }
        LOG.error("unexpected error", t);
        return Errors.UNKNOWN;
    }
}
