package io.epochlog.client;

import io.epochlog.common.TopicPartition;

/**
 * Where a sent record landed. With {@code acks=0} the offset and timestamp are -1.
 *
 * <p>Thread-safety: immutable.
 */
public record RecordMetadata(TopicPartition topicPartition, long offset, long timestamp) {}
