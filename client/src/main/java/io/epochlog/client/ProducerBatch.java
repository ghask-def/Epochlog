package io.epochlog.client;

import io.epochlog.common.Header;
import io.epochlog.common.RecordBatch;
import io.epochlog.common.RecordBatchBuilder;
import io.epochlog.common.TopicPartition;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Records headed for one partition, sent together. The batch is sealed (built, and given a
 * producer id and sequence) the first time it is drained; retries resend the exact same bytes so
 * the broker can recognise duplicates.
 *
 * <p>Thread-safety: not thread-safe. Guarded by the owning partition queue's monitor until
 * sealed; immutable afterwards apart from retry bookkeeping done by the sender.
 */
final class ProducerBatch {
    final TopicPartition tp;
    final long createdMs;
    final long deadlineMs;
    private final RecordBatchBuilder builder;
    private final List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
    private int bytes;
    private RecordBatch sealed;
    int attempts;
    long retryAtMs;

    ProducerBatch(TopicPartition tp, RecordBatchBuilder builder, long now, long deliveryTimeoutMs) {
        this.tp = tp;
        this.builder = builder;
        this.createdMs = now;
        this.deadlineMs = now + deliveryTimeoutMs;
    }

    /** Returns null if the record doesn't fit. A batch always accepts its first record. */
    CompletableFuture<RecordMetadata> tryAppend(byte[] key, byte[] value, List<Header> headers, int size, int batchSize) {
        if (sealed != null || (!futures.isEmpty() && builder.estimatedSizeInBytes() + size > batchSize)) {
            return null;
        }
        builder.append(key, value, headers);
        CompletableFuture<RecordMetadata> f = new CompletableFuture<>();
        futures.add(f);
        bytes += size;
        return f;
    }

    boolean isFull(int batchSize) {
        return sealed != null || builder.estimatedSizeInBytes() >= batchSize;
    }

    boolean isSealed() {
        return sealed != null;
    }

    int recordCount() {
        return futures.size();
    }

    int bytes() {
        return bytes;
    }

    RecordBatch seal(long producerId, int sequence, long timestamp) {
        if (sealed == null) {
            if (producerId >= 0) {
                builder.producer(producerId, sequence);
            }
            builder.baseTimestamp(timestamp);
            sealed = builder.build();
        }
        return sealed;
    }

    RecordBatch sealedBatch() {
        return sealed;
    }

    void complete(long baseOffset, long baseTimestamp) {
        for (int i = 0; i < futures.size(); i++) {
            long offset = baseOffset < 0 ? -1 : baseOffset + i;
            long ts = baseTimestamp < 0 ? -1 : baseTimestamp + i;
            futures.get(i).complete(new RecordMetadata(tp, offset, ts));
        }
    }

    void fail(Throwable t) {
        for (CompletableFuture<RecordMetadata> f : futures) {
            f.completeExceptionally(t);
        }
    }
}
