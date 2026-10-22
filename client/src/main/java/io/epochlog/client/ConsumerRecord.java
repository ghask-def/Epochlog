package io.epochlog.client;

import io.epochlog.common.Header;
import io.epochlog.common.HybridTime;
import io.epochlog.common.TopicPartition;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A record returned by {@link Consumer#poll}.
 *
 * <p>Thread-safety: immutable as long as callers don't mutate the arrays.
 */
public record ConsumerRecord(String topic, int partition, long offset, long timestamp, byte[] key, byte[] value,
        List<Header> headers) {

    public TopicPartition topicPartition() {
        return new TopicPartition(topic, partition);
    }

    public String keyString() {
        return key == null ? null : new String(key, StandardCharsets.UTF_8);
    }

    public String valueString() {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        return topic + "-" + partition + "@" + offset + " [" + HybridTime.format(timestamp) + "] "
                + keyString() + "=" + valueString();
    }
}
