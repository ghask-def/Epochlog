package io.epochlog.broker;

import io.epochlog.concurrent.DelayedOperation;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.message.ProduceResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * An {@code acks=all} produce waiting for the high watermark to pass every partition's appended
 * batch. Lives in the produce purgatory keyed by partition; completes when all partitions have
 * an answer or the request timeout fires.
 *
 * <p>Thread-safety: thread-safe. {@link #tryComplete} may run concurrently on several threads;
 * per-partition outcomes are recorded idempotently and completion is guarded by the base class.
 */
final class DelayedProduce extends DelayedOperation {

    static final class PartitionStatus {
        final Partition partition;
        final long requiredOffset;
        final ProduceResponse.PartitionResult appended;
        volatile Errors outcome;

        PartitionStatus(Partition partition, long requiredOffset, ProduceResponse.PartitionResult appended) {
            this.partition = partition;
            this.requiredOffset = requiredOffset;
            this.appended = appended;
        }
    }

    private final List<PartitionStatus> pending;
    private final List<ProduceResponse.PartitionResult> alreadyDone;
    private final int minInsync;
    private final Consumer<ProduceResponse> respond;

    DelayedProduce(long timeoutMs, List<PartitionStatus> pending, List<ProduceResponse.PartitionResult> alreadyDone,
            int minInsync, Consumer<ProduceResponse> respond) {
        super(timeoutMs);
        this.pending = pending;
        this.alreadyDone = alreadyDone;
        this.minInsync = minInsync;
        this.respond = respond;
    }

    @Override
    public boolean tryComplete() {
        boolean allDone = true;
        for (PartitionStatus s : pending) {
            if (s.outcome != null) {
                continue;
            }
            Errors e = s.partition.checkEnoughReplicasReachOffset(s.requiredOffset, minInsync);
            if (e == null) {
                allDone = false;
            } else {
                s.outcome = e;
            }
        }
        return allDone && forceComplete();
    }

    @Override
    protected void onComplete() {
        List<ProduceResponse.PartitionResult> results = new ArrayList<>(alreadyDone);
        for (PartitionStatus s : pending) {
            Errors e = s.outcome == null ? Errors.REQUEST_TIMED_OUT : s.outcome;
            ProduceResponse.PartitionResult a = s.appended;
            results.add(e == Errors.NONE ? a
                    : new ProduceResponse.PartitionResult(a.topic(), a.partition(), e, -1, -1));
        }
        respond.accept(new ProduceResponse(results));
    }
}
