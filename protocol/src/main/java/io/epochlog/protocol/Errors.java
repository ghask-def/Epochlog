package io.epochlog.protocol;

/**
 * Error codes returned in responses. Retriable errors are ones a client can expect to clear up
 * on its own after refreshing metadata or backing off.
 *
 * <p>Thread-safety: immutable.
 */
public enum Errors {
    UNKNOWN(-1, false),
    NONE(0, false),
    OFFSET_OUT_OF_RANGE(1, false),
    CORRUPT_MESSAGE(2, true),
    UNKNOWN_TOPIC_OR_PARTITION(3, true),
    NOT_LEADER_OR_FOLLOWER(4, true),
    REQUEST_TIMED_OUT(5, true),
    BROKER_NOT_AVAILABLE(6, true),
    NETWORK_EXCEPTION(7, true),
    MESSAGE_TOO_LARGE(8, false),
    NOT_ENOUGH_REPLICAS(9, true),
    NOT_ENOUGH_REPLICAS_AFTER_APPEND(10, true),
    INVALID_REQUEST(11, false),
    TOPIC_ALREADY_EXISTS(12, false),
    INVALID_PARTITIONS(13, false),
    INVALID_REPLICATION_FACTOR(14, false),
    OUT_OF_ORDER_SEQUENCE_NUMBER(15, false),
    DUPLICATE_SEQUENCE_NUMBER(16, false),
    UNKNOWN_PRODUCER_ID(17, false),
    FENCED_LEADER_EPOCH(18, true),
    UNKNOWN_LEADER_EPOCH(19, true),
    STALE_BROKER_EPOCH(20, true),
    STALE_CONTROLLER_EPOCH(21, false),
    INVALID_UPDATE_VERSION(22, true),
    STORAGE_ERROR(23, true),
    COORDINATOR_NOT_AVAILABLE(24, true),
    NOT_CONTROLLER(25, true),
    INVALID_TOPIC(26, false);

    public final short code;
    private final boolean retriable;

    Errors(int code, boolean retriable) {
        this.code = (short) code;
        this.retriable = retriable;
    }

    public boolean retriable() {
        return retriable;
    }

    public static Errors forCode(short code) {
        for (Errors e : values()) {
            if (e.code == code) {
                return e;
            }
        }
        return UNKNOWN;
    }

    public EpochlogApiException exception(String message) {
        return new EpochlogApiException(this, message);
    }

    public EpochlogApiException exception() {
        return new EpochlogApiException(this, name());
    }
}
