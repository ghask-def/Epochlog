package io.epochlog.client;

import io.epochlog.common.Header;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A record to send. A null partition means pick one: by key hash if there is a key, otherwise
 * stick to one partition until its batch fills.
 *
 * <p>Thread-safety: immutable as long as callers don't mutate the arrays.
 */
public record ProducerRecord(String topic, Integer partition, byte[] key, byte[] value, List<Header> headers) {

    public ProducerRecord {
        headers = headers == null ? List.of() : List.copyOf(headers);
    }

    public static ProducerRecord of(String topic, String key, String value) {
        return new ProducerRecord(topic, null, key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                value == null ? null : value.getBytes(StandardCharsets.UTF_8), List.of());
    }

    public static ProducerRecord of(String topic, int partition, byte[] key, byte[] value) {
        return new ProducerRecord(topic, partition, key, value, List.of());
    }
}
