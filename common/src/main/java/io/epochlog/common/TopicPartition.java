package io.epochlog.common;

import java.util.Objects;

/**
 * A topic name and partition index.
 *
 * <p>Thread-safety: immutable.
 */
public record TopicPartition(String topic, int partition) implements Comparable<TopicPartition> {

    public TopicPartition {
        Objects.requireNonNull(topic, "topic");
        if (partition < 0) {
            throw new IllegalArgumentException("negative partition " + partition);
        }
    }

    /** Parses the directory form produced by {@link #toString()}, e.g. {@code orders-3}. */
    public static TopicPartition parse(String s) {
        int dash = s.lastIndexOf('-');
        if (dash <= 0 || dash == s.length() - 1) {
            throw new IllegalArgumentException("not a topic partition: " + s);
        }
        return new TopicPartition(s.substring(0, dash), Integer.parseInt(s.substring(dash + 1)));
    }

    @Override
    public int compareTo(TopicPartition o) {
        int c = topic.compareTo(o.topic);
        return c != 0 ? c : Integer.compare(partition, o.partition);
    }

    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}
