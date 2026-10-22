package io.epochlog.broker;

import io.epochlog.clock.HybridLogicalClock;
import io.epochlog.common.AtomicFiles;
import io.epochlog.common.RecordBatch;
import io.epochlog.common.Time;
import io.epochlog.common.TopicPartition;
import io.epochlog.concurrent.GroupCommitter;
import io.epochlog.concurrent.SingleWriterExecutor;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.MemoryRecords;
import io.epochlog.protocol.message.AlterIsrResponse;
import io.epochlog.replication.FollowerReplica;
import io.epochlog.replication.HighWatermark;
import io.epochlog.replication.LeaderReplication;
import io.epochlog.storage.FileSlice;
import io.epochlog.storage.Log;
import io.epochlog.storage.LogAppendInfo;
import io.epochlog.storage.OffsetAndEpoch;
import io.epochlog.storage.TimestampAndOffset;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One partition replica on this broker: the log, its single writer thread, leadership state,
 * replication state when leading, and the high watermark.
 *
 * <p>Thread-safety: thread-safe. Every log mutation (leader append, follower append, truncation,
 * role change) runs on the partition's {@link SingleWriterExecutor}; request handlers and the
 * replica fetcher enqueue work and get a future. Leadership fields are volatile and only
 * written on the writer thread. The high watermark is advanced with a CAS from whichever thread
 * learns something new (see {@link HighWatermark}).
 */
public final class Partition implements FollowerReplica, AutoCloseable {
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

    /**
     * Sends ISR change proposals to the controller.
     *
     * <p>Thread-safety: implementations must be thread-safe.
     */
    public interface IsrChannel {
        CompletableFuture<AlterIsrResponse.PartitionResult> alterIsr(TopicPartition tp, int leaderEpoch,
                int partitionEpoch, List<Integer> newIsr);
    }

    private final TopicPartition tp;
    private final Log log;
    private final int localBrokerId;
    private final HybridLogicalClock clock;
    private final GroupCommitter committer;
    private final GroupCommitter.Syncable syncable;
    private final Listener listener;
    private final IsrChannel isrChannel;
    private final Time time;
    private final SingleWriterExecutor writer;
    private final HighWatermark highWatermark;

    private volatile int leaderId = -1;
    private volatile int leaderEpoch = -1;
    private volatile int partitionEpoch = -1;
    private volatile List<Integer> replicas = List.of();
    private volatile List<Integer> followerIsr = List.of();
    private volatile LeaderReplication leaderState;
    private volatile long leaderEpochStartOffset;
    private long lastNotifiedLogEnd = -1;

    public Partition(TopicPartition tp, Log log, int localBrokerId, HybridLogicalClock clock, GroupCommitter committer,
            Listener listener, IsrChannel isrChannel, Time time) throws IOException {
        this.tp = tp;
        this.log = log;
        this.localBrokerId = localBrokerId;
        this.clock = clock;
        this.committer = committer;
        this.syncable = log::flush;
        this.listener = listener;
        this.isrChannel = isrChannel;
        this.time = time;
        this.highWatermark = new HighWatermark(Math.min(readHighWatermarkCheckpoint(), log.logEndOffset()));
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

    @Override
    public TopicPartition topicPartition() {
        return tp;
    }

    public Log log() {
        return log;
    }

    public boolean isLeader() {
        return leaderId == localBrokerId && leaderState != null;
    }

    public int leaderId() {
        return leaderId;
    }

    @Override
    public int leaderEpoch() {
        return leaderEpoch;
    }

    public int partitionEpoch() {
        return partitionEpoch;
    }

    public List<Integer> replicas() {
        return replicas;
    }

    /** The ISR the controller has accepted. */
    public List<Integer> isr() {
        LeaderReplication ls = leaderState;
        return ls != null ? ls.isrState().isr() : followerIsr;
    }

    LeaderReplication leaderState() {
        return leaderState;
    }

    @Override
    public long highWatermark() {
        return highWatermark.get();
    }

    @Override
    public long logEndOffset() {
        return log.logEndOffset();
    }

    @Override
    public int latestLogEpoch() {
        return log.latestEpoch();
    }

    @Override
    public OffsetAndEpoch endOffsetForEpoch(int epoch) {
        return log.endOffsetForEpoch(epoch);
    }

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

    public CompletableFuture<Void> becomeLeader(int epoch, int newPartitionEpoch, List<Integer> newReplicas,
            List<Integer> newIsr) {
        return onWriter(() -> {
            long leo = log.logEndOffset();
            leaderEpoch = epoch;
            partitionEpoch = newPartitionEpoch;
            replicas = List.copyOf(newReplicas);
            followerIsr = List.of();
            leaderEpochStartOffset = leo;
            // A new term starts at the current log end; followers truncate against this boundary.
            log.epochCache().assign(epoch, leo);
            leaderState = new LeaderReplication(localBrokerId, newReplicas, newIsr, newPartitionEpoch,
                    time.milliseconds());
            leaderId = localBrokerId;
            LOG.info("{}: became leader at epoch {} with ISR {}, log end {}, high watermark {}", tp, epoch, newIsr,
                    leo, highWatermark.get());
            maybeIncrementHighWatermark();
            return null;
        });
    }

    public CompletableFuture<Void> becomeFollower(int newLeader, int epoch, int newPartitionEpoch,
            List<Integer> newReplicas, List<Integer> newIsr) {
        return onWriter(() -> {
            boolean wasLeader = leaderState != null;
            leaderState = null;
            leaderId = newLeader;
            leaderEpoch = epoch;
            partitionEpoch = newPartitionEpoch;
            replicas = List.copyOf(newReplicas);
            followerIsr = List.copyOf(newIsr);
            LOG.info("{}: following broker {} at epoch {}, log end {}", tp, newLeader, epoch, log.logEndOffset());
            if (wasLeader) {
                // Wake anything parked on us so it can fail with NOT_LEADER.
                listener.onHighWatermarkChanged(tp);
            }
            return null;
        });
    }

    /** Same leader epoch, new ISR from the controller (e.g. it removed a dead follower). */
    public CompletableFuture<Void> updateIsr(int newPartitionEpoch, List<Integer> newReplicas, List<Integer> newIsr) {
        return onWriter(() -> {
            partitionEpoch = Math.max(partitionEpoch, newPartitionEpoch);
            replicas = List.copyOf(newReplicas);
            LeaderReplication ls = leaderState;
            if (ls != null) {
                ls.commit(newIsr, newPartitionEpoch);
                maybeIncrementHighWatermark();
            } else {
                followerIsr = List.copyOf(newIsr);
            }
            return null;
        });
    }

    /** Appends a producer batch as leader. Completes once in the log and, with group commit, fsynced. */
    public CompletableFuture<LogAppendInfo> appendAsLeader(RecordBatch batch) {
        CompletableFuture<LogAppendInfo> result = new CompletableFuture<>();
        writer.execute(() -> {
            if (!isLeader()) {
                result.completeExceptionally(Errors.NOT_LEADER_OR_FOLLOWER.exception(tp + " is led by " + leaderId));
                return;
            }
            LogAppendInfo info;
            try {
                if (batch.baseTimestamp() >= 0) {
                    // The producer's clock reading is a receive event for our HLC. Readings too far
                    // ahead are counted and ignored by observe().
                    clock.observe(batch.maxTimestamp());
                }
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

    boolean maybeIncrementHighWatermark() {
        LeaderReplication ls = leaderState;
        if (ls == null) {
            return false;
        }
        if (highWatermark.advanceTo(ls.computeHighWatermark(leaderDurableEnd()))) {
            listener.onHighWatermarkChanged(tp);
            return true;
        }
        return false;
    }

    /** Leader side of a follower fetch: record its progress, then maybe grow the ISR and the watermark. */
    public void onFollowerFetch(int replicaId, long fetchOffset) {
        LeaderReplication ls = leaderState;
        if (ls == null || !ls.onFollowerFetch(replicaId, fetchOffset, log.logEndOffset(), time.milliseconds())) {
            return;
        }
        if (ls.canJoinIsr(replicaId, highWatermark.get(), leaderEpochStartOffset)) {
            List<Integer> grown = new ArrayList<>(ls.isrState().isr());
            grown.add(replicaId);
            proposeIsr(ls, grown);
        }
        maybeIncrementHighWatermark();
    }

    /** Drops followers that have fallen too far behind. Called periodically. */
    public void maybeShrinkIsr(long maxLagMs) {
        LeaderReplication ls = leaderState;
        if (ls == null) {
            return;
        }
        List<Integer> lagging = ls.laggingFollowers(time.milliseconds(), maxLagMs);
        if (lagging.isEmpty()) {
            return;
        }
        List<Integer> shrunk = new ArrayList<>(ls.isrState().isr());
        shrunk.removeAll(lagging);
        LOG.info("{}: followers {} are lagging, proposing ISR {}", tp, lagging, shrunk);
        proposeIsr(ls, shrunk);
    }

    private void proposeIsr(LeaderReplication ls, List<Integer> newIsr) {
        if (isrChannel == null || !ls.propose(newIsr)) {
            return;
        }
        int epoch = leaderEpoch;
        int pe = ls.isrState().partitionEpoch();
        isrChannel.alterIsr(tp, epoch, pe, newIsr).whenComplete((r, err) -> {
            if (leaderState != ls) {
                return;
            }
            if (err == null && r.error() == Errors.NONE) {
                ls.commit(r.isr(), r.partitionEpoch());
                partitionEpoch = Math.max(partitionEpoch, r.partitionEpoch());
                LOG.info("{}: ISR is now {}", tp, r.isr());
                maybeIncrementHighWatermark();
            } else {
                LOG.info("{}: ISR change to {} rejected: {}", tp, newIsr, err != null ? err.toString() : r.error());
                ls.abortPending();
            }
        });
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
        return isr().size() < minInsync ? Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND : Errors.NONE;
    }

    @Override
    public CompletableFuture<Void> appendAsFollower(MemoryRecords records, long leaderHighWatermark, int epoch) {
        return onWriter(() -> {
            if (isLeader() || epoch != leaderEpoch) {
                return null;
            }
            for (RecordBatch batch : records.batches()) {
                long leo = log.logEndOffset();
                if (batch.lastOffset() < leo) {
                    continue;
                }
                if (batch.baseOffset() != leo) {
                    throw new IllegalStateException(tp + ": leader sent batch at " + batch.baseOffset()
                            + " but our log ends at " + leo);
                }
                log.appendAsFollower(batch);
                clock.observe(batch.maxTimestamp());
            }
            highWatermark.advanceTo(Math.min(leaderHighWatermark, log.logEndOffset()));
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> truncateTo(long offset, int epoch) {
        return onWriter(() -> {
            if (isLeader() || epoch != leaderEpoch) {
                return null;
            }
            log.truncateTo(offset);
            if (highWatermark.get() > log.logEndOffset()) {
                highWatermark.set(log.logEndOffset());
            }
            return null;
        });
    }

    @Override
    public CompletableFuture<Void> truncateFullyAndStartAt(long offset, int epoch) {
        return onWriter(() -> {
            if (isLeader() || epoch != leaderEpoch) {
                return null;
            }
            log.truncateFullyAndStartAt(offset);
            highWatermark.set(offset);
            return null;
        });
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
        return "Partition(" + tp + ", leader=" + leaderId + ", epoch=" + leaderEpoch + ", isr=" + isr()
                + ", hw=" + highWatermark.get() + ", leo=" + log.logEndOffset() + ")";
    }
}
