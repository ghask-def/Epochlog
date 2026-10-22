package io.epochlog.broker;

import io.epochlog.common.TopicPartition;
import io.epochlog.network.NetworkClient;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.message.AlterIsrRequest;
import io.epochlog.protocol.message.AlterIsrResponse;
import io.epochlog.protocol.message.BrokerInfo;
import io.epochlog.protocol.message.CreateTopicRequest;
import io.epochlog.protocol.message.ErrorResponse;
import io.epochlog.protocol.message.HeartbeatRequest;
import io.epochlog.protocol.message.HeartbeatResponse;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The broker's connection to the controller: periodic heartbeats, ISR change proposals and
 * forwarded topic creation.
 *
 * <p>Heartbeats double as a lease. The controller declares a broker dead {@code sessionTimeout}
 * after the last heartbeat it received. The broker measures from when it <em>sent</em> the last
 * heartbeat that got acknowledged, which is never later than when the controller received it,
 * and stops accepting writes a little before {@code sessionTimeout} runs out. So a leader cut off
 * from the controller fences itself before a replacement can be elected, and a zombie leader
 * can't acknowledge writes the new leader will never see.
 *
 * <p>Thread-safety: thread-safe.
 */
final class ControllerChannel implements Partition.IsrChannel, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ControllerChannel.class);

    private final int brokerId;
    private final InetSocketAddress controller;
    private final NetworkClient client;
    private final Supplier<BrokerInfo> self;
    private final LongSupplier metadataVersion;
    private final long heartbeatIntervalMs;
    private final long leaseMs;
    private final Thread thread;
    private volatile long brokerEpoch = -1;
    private volatile long lastAckedSendNanos = Long.MIN_VALUE / 2;
    private volatile boolean running = true;

    ControllerChannel(int brokerId, InetSocketAddress controller, NetworkClient client, Supplier<BrokerInfo> self,
            LongSupplier metadataVersion, long heartbeatIntervalMs, long sessionTimeoutMs) {
        this.brokerId = brokerId;
        this.controller = controller;
        this.client = client;
        this.self = self;
        this.metadataVersion = metadataVersion;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        // Leave a margin for clock rate differences between the two machines.
        this.leaseMs = sessionTimeoutMs * 3 / 4;
        this.thread = Thread.ofPlatform().name("controller-channel-" + brokerId).daemon().unstarted(this::run);
    }

    void start() {
        thread.start();
    }

    private void run() {
        while (running) {
            long sentAt = System.nanoTime();
            BrokerInfo me = self.get();
            try {
                HeartbeatResponse resp = client.send(controller, new HeartbeatRequest(brokerId, me.host(), me.port(),
                        brokerEpoch, metadataVersion.getAsLong()), HeartbeatResponse::read)
                        .get(Math.max(heartbeatIntervalMs * 2, 1000), TimeUnit.MILLISECONDS);
                if (resp.error() == Errors.NONE) {
                    if (resp.brokerEpoch() != brokerEpoch) {
                        LOG.info("broker {} registered with controller, broker epoch {}", brokerId, resp.brokerEpoch());
                    }
                    brokerEpoch = resp.brokerEpoch();
                    lastAckedSendNanos = Math.max(lastAckedSendNanos, sentAt);
                }
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                LOG.debug("heartbeat to controller failed: {}", e.toString());
            }
            long elapsed = (System.nanoTime() - sentAt) / 1_000_000;
            try {
                Thread.sleep(Math.max(0, heartbeatIntervalMs - elapsed));
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    /** True while the controller still considers us alive, so we may act as leader. */
    boolean leaseValid() {
        return System.nanoTime() - lastAckedSendNanos < TimeUnit.MILLISECONDS.toNanos(leaseMs);
    }

    long brokerEpoch() {
        return brokerEpoch;
    }

    @Override
    public CompletableFuture<AlterIsrResponse.PartitionResult> alterIsr(TopicPartition tp, int leaderEpoch,
            int partitionEpoch, List<Integer> newIsr) {
        AlterIsrRequest req = new AlterIsrRequest(brokerId, brokerEpoch, List.of(
                new AlterIsrRequest.PartitionData(tp.topic(), tp.partition(), leaderEpoch, partitionEpoch, newIsr)));
        return client.send(controller, req, AlterIsrResponse::read).thenApply(resp -> {
            if (resp.error() != Errors.NONE || resp.partitions().isEmpty()) {
                Errors e = resp.error() != Errors.NONE ? resp.error() : Errors.UNKNOWN;
                return new AlterIsrResponse.PartitionResult(tp.topic(), tp.partition(), e, leaderEpoch, partitionEpoch,
                        newIsr);
            }
            return resp.partitions().get(0);
        });
    }

    CompletableFuture<ErrorResponse> createTopic(CreateTopicRequest req) {
        return client.send(controller, req, ErrorResponse::read);
    }

    @Override
    public void close() {
        running = false;
        thread.interrupt();
        try {
            thread.join(2_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
