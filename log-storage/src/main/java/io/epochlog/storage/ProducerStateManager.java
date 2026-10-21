package io.epochlog.storage;

import io.epochlog.common.RecordBatch;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

/**
 * Tracks the last few batches written by each idempotent producer so retries can be detected.
 * A batch is accepted if its base sequence follows the producer's last sequence; a batch that
 * matches one of the last {@value #BATCHES_TO_RETAIN} batches exactly is a duplicate.
 *
 * <p>Thread-safety: not thread-safe. Owned by the partition writer thread.
 */
public final class ProducerStateManager {
    public static final int BATCHES_TO_RETAIN = 5;

    /**
     * Sequence and offset range of one batch.
     *
     * <p>Thread-safety: immutable.
     */
    public record BatchMetadata(int firstSeq, int lastSeq, long firstOffset, long lastOffset, long maxTimestamp) {}

    /** Outcome of checking a batch. */
    public enum Verdict { ACCEPT, DUPLICATE }

    /**
     * Check result; {@code original} is set for duplicates.
     *
     * <p>Thread-safety: immutable.
     */
    public record Check(Verdict verdict, BatchMetadata original) {
        static final Check ACCEPT = new Check(Verdict.ACCEPT, null);
    }

    private final Map<Long, ArrayDeque<BatchMetadata>> producers = new HashMap<>();

    /**
     * @throws OutOfOrderSequenceException if the sequence leaves a gap or goes backwards
     * @throws UnknownProducerIdException if the producer is new and doesn't start at zero
     */
    public Check check(RecordBatch batch) {
        if (!batch.hasProducerId()) {
            return Check.ACCEPT;
        }
        long pid = batch.producerId();
        int firstSeq = batch.baseSequence();
        int lastSeq = batch.lastSequence();
        ArrayDeque<BatchMetadata> history = producers.get(pid);
        if (history == null || history.isEmpty()) {
            if (firstSeq != 0) {
                throw new UnknownProducerIdException("no state for producer " + pid + ", first sequence " + firstSeq);
            }
            return Check.ACCEPT;
        }
        for (BatchMetadata m : history) {
            if (m.firstSeq() == firstSeq && m.lastSeq() == lastSeq) {
                return new Check(Verdict.DUPLICATE, m);
            }
        }
        int expected = history.peekLast().lastSeq() + 1;
        if (firstSeq != expected) {
            throw new OutOfOrderSequenceException("producer " + pid + " expected sequence " + expected
                    + " but got " + firstSeq);
        }
        return Check.ACCEPT;
    }

    /** Records an appended batch. No validation; followers call this for replicated data too. */
    public void update(RecordBatch batch) {
        if (!batch.hasProducerId() || batch.baseSequence() < 0) {
            return;
        }
        ArrayDeque<BatchMetadata> history = producers.computeIfAbsent(batch.producerId(), k -> new ArrayDeque<>());
        history.addLast(new BatchMetadata(batch.baseSequence(), batch.lastSequence(), batch.baseOffset(),
                batch.lastOffset(), batch.maxTimestamp()));
        while (history.size() > BATCHES_TO_RETAIN) {
            history.removeFirst();
        }
    }

    public void clear() {
        producers.clear();
    }

    public int producerCount() {
        return producers.size();
    }

    /** Last sequence written by {@code producerId}, or -1. */
    public int lastSequence(long producerId) {
        ArrayDeque<BatchMetadata> history = producers.get(producerId);
        return history == null || history.isEmpty() ? -1 : history.peekLast().lastSeq();
    }
}
