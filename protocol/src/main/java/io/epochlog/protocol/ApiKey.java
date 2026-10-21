package io.epochlog.protocol;

/**
 * Request types. The id goes on the wire in every request header.
 *
 * <p>Thread-safety: immutable.
 */
public enum ApiKey {
    PRODUCE(0),
    FETCH(1),
    LIST_OFFSETS(2),
    METADATA(3),
    CREATE_TOPIC(4),
    REPLICA_FETCH(5),
    LEADER_AND_ISR(6),
    HEARTBEAT(7),
    OFFSET_FOR_LEADER_EPOCH(8),
    INIT_PRODUCER_ID(9),
    ALTER_ISR(10),
    OFFSET_COMMIT(11),
    OFFSET_FETCH(12);

    private static final ApiKey[] BY_ID = new ApiKey[values().length];

    static {
        for (ApiKey k : values()) {
            BY_ID[k.id] = k;
        }
    }

    public final short id;

    ApiKey(int id) {
        this.id = (short) id;
    }

    public static ApiKey forId(int id) {
        if (id < 0 || id >= BY_ID.length || BY_ID[id] == null) {
            throw new IllegalArgumentException("unknown api key " + id);
        }
        return BY_ID[id];
    }
}
