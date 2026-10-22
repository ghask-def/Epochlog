package io.epochlog.broker;

import io.epochlog.clock.HybridLogicalClock;
import io.epochlog.common.AtomicFiles;
import io.epochlog.common.RecordBatch;
import io.epochlog.common.Time;
import io.epochlog.common.TopicPartition;
import io.epochlog.concurrent.GroupCommitter;
import io.epochlog.concurrent.SingleWriterExecutor;
import io.epochlog.protocol.Errors;
import io.epochlog.storage.FileSlice;
import io.epochlog.storage.Log;
import io.epochlog.storage.LogAppendInfo;
import io.epochlog.storage.TimestampAndOffset;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One partition replica on this broker: the log, its single writer thread, leadership state and
 * the high watermark.
 *
 * <p>Thread-safety: thread-safe. Every log mutation (append, truncation, role change) runs on
 * the partition's {@link SingleWriterExecutor}; request handlers enqueue work and get a future.
 * Leadership fields are volatile and only written on the writer thread. The high watermark is
 * an {@link AtomicLong} that only moves forward while leader, so concurrent updaters can each
 * try to raise it with a CAS.
 */
public final class Partition implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(Partition.class);
    static final String HW_FILE = "high-watermark";

    /**
     * Callbacks to the replica manager.
     *
     * <p>Thread-safety: called from writer, committer and handler threads.
     */
    public interface Listener {
        void onHighWatermarkChanged(TopicPartition tp);

        void onLogEndOffsetChanged(TopicPartition tp);
    }

    private final TopicPartition tp;
    private final Log log;
    private final int localBrokerId;
    private final HybridLogicalClock clock;
    private final GroupCommitter committer;
    private final GroupCommitter.Syncable syncable;
    private final Listener listener;
    private final Time time;
    private final SingleWriterExecutor writer;
    private final AtomicLong highWatermark = new AtomicLong();

    private volatile int leaderId = -1;
    private volatile int leaderEpoch = -1;
    private volatile int partitionEpoch;
    private volatile List<Integer> replicas = List.of();
    private volatile List<Integer> isr = List.of();
    private long lastNotifiedLogEnd = -1;

    public Partition(TopicPartition tp, Log log, int localBrokerId, HybridLogicalClock clock, GroupCommitter committer,
            Listener listener, Time time) throws IOException {
        this.tp = tp;
        this.log = log;
        this.localBrokerId = localBrokerId;
        this.clock = clock;
        this.committer = committer;
        this.syncable = log::flush;
        this.listener = listener;
        this.time = time;
        this.highWatermark.set(Math.min(readHighWatermarkCheckpoint(), log.logEndOffset()));
        this.writer = new SingleWriterExecutor("writer-" + tp, 8192, this::afterWriteBatch);
    }

    private long readHighWatermarkCheckpoint() throws IOException {
        Path p = log.dir().resolve(HW_FILE);
        if (!Files.exists(p)) {
            return log.logStartOffset();
        }
        return Math.max(log.logStartOffset(), Long.parseLong(Files.readString(p).trim()));
    }

    public void checkpointHighWatermark() throws IOException {
        AtomicFiles.write(log.dir().resolve(HW_FILE), Long.toString(highWatermark.get()));
    }

    public TopicPartition topicPartition() {
        return tp;
    }

    public Log log() {
        return log;
    }

    public boolean isLeader() {
        return leaderId == localBrokerId;
    }

    public int leaderId() {
        return leaderId;
    }

    public int leaderEpoch() {
        return leaderEpoch;
    }

    public int partitionEpoch() {
        return partitionEpoch;
    }

    public List<Integer> replicas() {
        return replicas;
    }

    public List<Integer> isr() {
        return isr;
    }

    public long highWatermark() {
        return highWatermark.get();
    }

    public long logEndOffset() {
        return log.logEndOffset();
    }

    /** Runs {@code task} on the writer thread and returns its result. */
    <T> CompletableFuture<T> onWriter(WriterTask<T> task) {
        CompletableFuture<T> f = new CompletableFuture<>();
        writer.execute(() -> {
            try {
                f.complete(task.run());
            } catch (Throwable t) {
                f.completeExceptionally(t);
            }
        });
        return f;
    }

    @FunctionalInterface
    interface WriterTask<T> {
        T run() throws Exception;
    }

    /** Makes this broker the leader. Single-node mode calls it with an ISR of just itself. */
    public CompletableFuture<Void> becomeLeader(int epoch, int newPartitionEpoch, List<Integer> newReplicas,
            List<Integer> newIsr) {
        return onWriter(() -> {
            leaderEpoch = epoch;
            partitionEpoch = newPartitionEpoch;
            replicas = List.copyOf(newReplicas);
            isr = List.copyOf(newIsr);
            leaderId = localBrokerId;
            // A new term starts at the current log end; record it so followers can truncate against it.
            log.epochCache().assign(epoch, log.logEndOffset());
            LOG.info("{}: became leader at epoch {} with ISR {}, log end {}, high watermark {}", tp, epoch, newIsr,
                    log.logEndOffset(), highWatermark.get());
            maybeIncrementHighWatermark();
            return null;
        });
    }

    /**
     * Appends a producer batch as leader. Completes once the batch is in the log and, with group
     * commit on, fsynced.
     */
    public CompletableFuture<LogAppendInfo> appendAsLeader(RecordBatch batch) {
        CompletableFuture<LogAppendInfo> result = new CompletableFuture<>();
        writer.execute(() -> {
            if (!isLeader()) {
                result.completeExceptionally(Errors.NOT_LEADER_OR_FOLLOWER.exception(tp + " is led by " + leaderId));
                return;
            }
            LogAppendInfo info;
            try {
                long stamp = clock.reserve(batch.lastOffsetDelta() + 1);
                info = log.appendAsLeader(batch, leaderEpoch, stamp);
                if (info.maxTimestamp() > clock.current()) {
                    // The log bumped the stamp past an earlier leader's timestamps; pull the clock along.
                    clock.observe(info.maxTimestamp());
                }
            } catch (Throwable t) {
                result.completeExceptionally(t);
                return;
            }
            if (committer == null) {
                result.complete(info);
                return;
            }
            committer.commit(syncable, info.lastOffset() + 1).whenComplete((v, err) -> {
                if (err != null) {
                    result.completeExceptionally(err);
                } else {
                    maybeIncrementHighWatermark();
                    result.complete(info);
                }
            });
        });
        return result;
    }

    private void afterWriteBatch() {
        long end = log.logEndOffset();
        if (end != lastNotifiedLogEnd) {
            lastNotifiedLogEnd = end;
            listener.onLogEndOffsetChanged(tp);
        }
        maybeIncrementHighWatermark();
    }

    /** The leader's own contribution to the high watermark: its log end, or its durable end with group commit. */
    long leaderDurableEnd() {
        return committer == null ? log.logEndOffset() : Math.min(log.recoveryPoint(), log.logEndOffset());
    }

    /** Recomputes the high watermark from the ISR and raises it if possible. */
    boolean maybeIncrementHighWatermark() {
        if (!isLeader()) {
            return false;
        }
        long candidate = leaderDurableEnd();
        long current = highWatermark.get();
        while (candidate > current) {
            if (highWatermark.compareAndSet(current, candidate)) {
                listener.onHighWatermarkChanged(tp);
                return true;
            }
            current = highWatermark.get();
        }
        return false;
    }

    /**
     * Whether a produce waiting for {@code requiredOffset} can be answered: null means keep
     * waiting, otherwise the error to return (NONE on success).
     */
    public Errors checkEnoughReplicasReachOffset(long requiredOffset, int minInsync) {
        if (!isLeader()) {
            return Errors.NOT_LEADER_OR_FOLLOWER;
        }
        if (highWatermark.get() < requiredOffset) {
            return null;
        }
        return isr.size() < minInsync ? Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND : Errors.NONE;
    }

    /** Reads for a consumer, bounded by the high watermark. */
    public FileSlice readCommitted(long fetchOffset, int maxBytes) throws IOException {
        return log.read(fetchOffset, maxBytes, highWatermark.get());
    }

    public TimestampAndOffset offsetForTimestamp(long hlc) throws IOException {
        return log.offsetForTimestamp(hlc, highWatermark.get());
    }

    @Override
    public void close() {
        writer.close();
        try {
            checkpointHighWatermark();
        } catch (IOException e) {
            LOG.warn("{}: failed to checkpoint high watermark", tp, e);
        }
    }

    /** Stops the writer thread without checkpointing anything. */
    public void closeAbruptly() {
        writer.close();
    }

    @Override
    public String toString() {
        return "Partition(" + tp + ", leader=" + leaderId + ", epoch=" + leaderEpoch + ", hw=" + highWatermark.get()
                + ", leo=" + log.logEndOffset() + ")";
    }
}
