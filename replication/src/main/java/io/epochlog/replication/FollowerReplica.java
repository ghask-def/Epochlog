package io.epochlog.replication;

import io.epochlog.common.TopicPartition;
import io.epochlog.protocol.MemoryRecords;
import io.epochlog.storage.OffsetAndEpoch;
import java.util.concurrent.CompletableFuture;

/**
 * The local side of a partition this broker follows. The broker's partition implements it;
 * mutating calls are queued to the partition's writer thread and complete asynchronously.
 *
 * <p>Thread-safety: implementations must be thread-safe.
 */
public interface FollowerReplica {
    TopicPartition topicPartition();

    /** The leader epoch the controller told us about. */
    int leaderEpoch();

    long logEndOffset();

    long highWatermark();

    /** Latest epoch present in the local log's epoch history, or -1. */
    int latestLogEpoch();

    OffsetAndEpoch endOffsetForEpoch(int epoch);

    /*
     * The mutations below carry the leader epoch the fetcher was working under. If leadership has
     * moved on since (the response raced a LeaderAndIsr), the replica ignores them.
     */

    CompletableFuture<Void> appendAsFollower(MemoryRecords records, long leaderHighWatermark, int epoch);

    CompletableFuture<Void> truncateTo(long offset, int epoch);

    CompletableFuture<Void> truncateFullyAndStartAt(long offset, int epoch);
}
