package io.epochlog.client;

import io.epochlog.common.TopicPartition;
import io.epochlog.network.NetworkClient;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.OffsetsTopic;
import io.epochlog.protocol.message.OffsetCommitRequest;
import io.epochlog.protocol.message.OffsetFetchRequest;
import io.epochlog.protocol.message.OffsetFetchResponse;
import io.epochlog.protocol.message.PartitionErrorsResponse;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Talks to a group's offset coordinator: the leader of the group's partition of the internal
 * offsets topic. Retries while the topic is being created or leadership is moving.
 *
 * <p>Thread-safety: thread-safe.
 */
final class OffsetClient {
    private final NetworkClient network;
    private final ClientMetadata metadata;
    private final long timeoutMs;
    private final long backoffMs;

    OffsetClient(NetworkClient network, ClientMetadata metadata, long timeoutMs, long backoffMs) {
        this.network = network;
        this.metadata = metadata;
        this.timeoutMs = timeoutMs;
        this.backoffMs = backoffMs;
    }

    private InetSocketAddress coordinator(String group) {
        int n = metadata.partitionCount(OffsetsTopic.NAME);
        if (n <= 0) {
            // Any broker will kick off creation of the offsets topic and tell us to retry.
            return metadata.anyBroker();
        }
        TopicPartition tp = new TopicPartition(OffsetsTopic.NAME, OffsetsTopic.partitionFor(group, n));
        return metadata.leaderFor(tp).map(b -> b.address()).orElse(metadata.anyBroker());
    }

    void commit(String group, Map<TopicPartition, Long> positions) throws InterruptedException, TimeoutException {
        if (positions.isEmpty()) {
            return;
        }
        List<OffsetCommitRequest.PartitionData> parts = new ArrayList<>();
        positions.forEach((tp, off) -> parts.add(new OffsetCommitRequest.PartitionData(tp.topic(), tp.partition(), off)));
        OffsetCommitRequest req = new OffsetCommitRequest(group, parts);
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            Errors error;
            try {
                PartitionErrorsResponse resp = network.send(coordinator(group), req, PartitionErrorsResponse::read)
                        .get(timeoutMs, TimeUnit.MILLISECONDS);
                error = resp.error();
            } catch (ExecutionException e) {
                error = Errors.NETWORK_EXCEPTION;
            }
            if (error == Errors.NONE) {
                return;
            }
            if (!error.retriable()) {
                throw error.exception("offset commit for group " + group);
            }
            backoff(deadline, "offset commit");
        }
    }

    OptionalLong fetch(String group, TopicPartition tp) throws InterruptedException, TimeoutException {
        OffsetFetchRequest req = new OffsetFetchRequest(group,
                List.of(new OffsetFetchRequest.PartitionData(tp.topic(), tp.partition())));
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            Errors error;
            try {
                OffsetFetchResponse resp = network.send(coordinator(group), req, OffsetFetchResponse::read)
                        .get(timeoutMs, TimeUnit.MILLISECONDS);
                error = resp.error();
                if (error == Errors.NONE) {
                    long off = resp.partitions().get(0).offset();
                    return off < 0 ? OptionalLong.empty() : OptionalLong.of(off);
                }
            } catch (ExecutionException e) {
                error = Errors.NETWORK_EXCEPTION;
            }
            if (!error.retriable()) {
                throw error.exception("offset fetch for group " + group);
            }
            backoff(deadline, "offset fetch");
        }
    }

    private void backoff(long deadline, String what) throws InterruptedException, TimeoutException {
        if (System.currentTimeMillis() > deadline) {
            throw new TimeoutException(what + " timed out");
        }
        Thread.sleep(backoffMs);
        metadata.refreshSync(timeoutMs);
    }
}
