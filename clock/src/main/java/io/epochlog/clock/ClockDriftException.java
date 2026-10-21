package io.epochlog.clock;

import io.epochlog.common.EpochlogException;

/**
 * A remote timestamp was further ahead of local physical time than the configured maximum drift.
 *
 * <p>Thread-safety: immutable once constructed.
 */
public class ClockDriftException extends EpochlogException {
    private final long driftMs;

    public ClockDriftException(String message, long driftMs) {
        super(message);
        this.driftMs = driftMs;
    }

    public long driftMs() {
        return driftMs;
    }
}
