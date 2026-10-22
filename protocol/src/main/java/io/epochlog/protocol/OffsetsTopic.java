package io.epochlog.protocol;

/**
 * The internal topic that stores consumer group offsets, and how groups map onto it.
 *
 * <p>Thread-safety: thread-safe (stateless).
 */
public final class OffsetsTopic {
    public static final String NAME = "__consumer_offsets";
    public static final int DEFAULT_PARTITIONS = 8;

    private OffsetsTopic() {}

    public static int partitionFor(String group, int partitions) {
        return Math.floorMod(group.hashCode(), partitions);
    }
}
