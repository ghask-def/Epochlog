package io.epochlog.client;

import io.epochlog.clock.HybridLogicalClock;
import io.epochlog.common.Header;
import io.epochlog.common.RecordBatchBuilder;
import io.epochlog.common.TopicPartition;
import io.epochlog.network.NetworkClient;
import io.epochlog.network.NetworkException;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.MemoryRecords;
import io.epochlog.protocol.message.BrokerInfo;
import io.epochlog.protocol.message.InitProducerIdRequest;
import io.epochlog.protocol.message.InitProducerIdResponse;
import io.epochlog.protocol.message.ProduceRequest;
import io.epochlog.protocol.message.ProduceResponse;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Asynchronous producer. {@link #send} appends to a per-partition batch and returns at once; a
 * background sender thread ships batches when they fill up or {@code linger.ms} passes, groups
 * them by leader into one request per broker, and retries retriable failures with backoff.
 *
 * <p>With idempotence on (the default) the producer gets a producer id and numbers each batch
 * per partition, so the broker drops retried duplicates. It then keeps one batch in flight per
 * partition, which keeps retries in order.
 *
 * <p>Thread-safety: thread-safe. Any number of threads may call {@link #send}.
 */
public final class Producer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

    private final ClientConfig config;
    private final NetworkClient network;
    private final ClientMetadata metadata;
    private final HybridLogicalClock clock = HybridLogicalClock.system();
    private final Semaphore bufferBytes;
    private final ConcurrentHashMap<TopicPartition, PartitionQueue> queues = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> stickyPartitions = new ConcurrentHashMap<>();
    private final AtomicInteger inFlightBatches = new AtomicInteger();
    private final Thread sender;
    private volatile long producerId = -1;
    private volatile boolean resetProducerId;
    private volatile boolean flushing;
    private volatile boolean closing;
    private volatile boolean running = true;

    private static final class PartitionQueue {
        final ArrayDeque<ProducerBatch> batches = new ArrayDeque<>();
        int inFlight;
        int nextSequence;
    }

    public Producer(ClientConfig config) {
        this(config, UnaryOperator.identity());
    }

    /**
     * @param resolver maps broker addresses to the addresses actually dialed; tests use it to
     *     route traffic through fault-injecting proxies
     */
    public Producer(ClientConfig config, UnaryOperator<InetSocketAddress> resolver) {
        this.config = config;
        this.network = new NetworkClient("producer-" + config.clientId(), config.clientId(), config.requestTimeoutMs(),
                resolver);
        this.metadata = new ClientMetadata(config.bootstrap(), network);
        this.bufferBytes = new Semaphore((int) Math.min(Integer.MAX_VALUE, config.bufferMemory()));
        this.sender = Thread.ofPlatform().name("producer-sender-" + config.clientId()).daemon().start(this::runSender);
    }

    public Producer(Map<String, String> props) {
        this(ClientConfig.from(props));
    }

    public CompletableFuture<RecordMetadata> send(ProducerRecord record) {
        if (closing) {
            return CompletableFuture.failedFuture(new IllegalStateException("producer is closed"));
        }
        try {
            int partitions = metadata.partitionCount(record.topic());
            if (partitions <= 0) {
                partitions = metadata.awaitTopic(record.topic(), config.maxBlockMs());
            }
            int partition = choosePartition(record, partitions);
            TopicPartition tp = new TopicPartition(record.topic(), partition);
            List<Header> headers = record.headers();
            int size = RecordBatchBuilder.estimateRecordSize(record.key(), record.value(), headers);
            if (!bufferBytes.tryAcquire(size, config.maxBlockMs(), TimeUnit.MILLISECONDS)) {
                throw new TimeoutException("buffer full for " + config.maxBlockMs() + " ms");
            }
            PartitionQueue q = queues.computeIfAbsent(tp, k -> new PartitionQueue());
            CompletableFuture<RecordMetadata> f;
            boolean wake;
            synchronized (q) {
                ProducerBatch last = q.batches.peekLast();
                f = last == null ? null : last.tryAppend(record.key(), record.value(), headers, size, config.batchSize());
                boolean newBatch = f == null;
                if (newBatch) {
                    if (last != null && record.partition() == null && record.key() == null) {
                        stickyPartitions.remove(record.topic());
                    }
                    ProducerBatch b = new ProducerBatch(tp, new RecordBatchBuilder(config.compression(), 0,
                            Math.min(config.batchSize(), Math.max(1024, size * 2))),
                            System.currentTimeMillis(), config.deliveryTimeoutMs());
                    f = b.tryAppend(record.key(), record.value(), headers, size, config.batchSize());
                    q.batches.addLast(b);
                }
                // A new batch needs its linger deadline scheduled; a full one should go now.
                wake = newBatch || q.batches.size() > 1 || q.batches.peekLast().isFull(config.batchSize());
            }
            if (wake || config.lingerMs() == 0) {
                LockSupport.unpark(sender);
            }
            return f;
        } catch (TimeoutException | InterruptedException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private int choosePartition(ProducerRecord record, int partitions) {
        if (record.partition() != null) {
            if (record.partition() < 0 || record.partition() >= partitions) {
                throw new IllegalArgumentException("partition " + record.partition() + " out of range for "
                        + record.topic());
            }
            return record.partition();
        }
        if (record.key() != null) {
            return Hashing.partitionFor(record.key(), partitions);
        }
        return stickyPartitions.computeIfAbsent(record.topic(),
                t -> java.util.concurrent.ThreadLocalRandom.current().nextInt(partitions)) % partitions;
    }

    private void runSender() {
        while (running) {
            try {
                long wait = runOnce();
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(Math.max(1, wait)));
            } catch (InterruptedException e) {
                return;
            } catch (RuntimeException e) {
                LOG.error("producer sender error", e);
            }
        }
    }

    private void ensureProducerId() throws InterruptedException {
        if (!config.idempotence() || (producerId >= 0 && !resetProducerId)) {
            return;
        }
        while (running) {
            try {
                InitProducerIdResponse resp = network.send(metadata.anyBroker(), new InitProducerIdRequest(),
                        InitProducerIdResponse::read).get(config.requestTimeoutMs(), TimeUnit.MILLISECONDS);
                if (resp.error() == Errors.NONE) {
                    for (PartitionQueue q : queues.values()) {
                        synchronized (q) {
                            q.nextSequence = 0;
                        }
                    }
                    producerId = resp.producerId();
                    resetProducerId = false;
                    return;
                }
            } catch (Exception e) {
                LOG.debug("InitProducerId failed, retrying", e);
            }
            Thread.sleep(config.retryBackoffMs());
        }
    }

    /** One pass over the queues. Returns how long the sender may sleep. */
    private long runOnce() throws InterruptedException {
        ensureProducerId();
        long now = System.currentTimeMillis();
        long nextWake = 100;
        boolean needMetadata = false;
        Map<BrokerInfo, List<ProducerBatch>> ready = new HashMap<>();
        int maxInFlight = config.idempotence() ? 1 : config.maxInFlightPerPartition();
        for (Map.Entry<TopicPartition, PartitionQueue> e : queues.entrySet()) {
            PartitionQueue q = e.getValue();
            synchronized (q) {
                ProducerBatch head;
                while ((head = q.batches.peekFirst()) != null && now > head.deadlineMs && !head.isSealed()) {
                    q.batches.pollFirst();
                    finish(head, new TimeoutException("batch for " + e.getKey() + " expired before it could be sent"));
                }
                if (head == null || q.inFlight >= maxInFlight) {
                    continue;
                }
                if (head.retryAtMs > now) {
                    nextWake = Math.min(nextWake, head.retryAtMs - now);
                    continue;
                }
                boolean sendable = head.isFull(config.batchSize()) || q.batches.size() > 1 || flushing || closing
                        || now - head.createdMs >= config.lingerMs();
                if (!sendable) {
                    nextWake = Math.min(nextWake, head.createdMs + config.lingerMs() - now);
                    continue;
                }
                BrokerInfo leader = metadata.leaderFor(e.getKey()).orElse(null);
                if (leader == null) {
                    needMetadata = true;
                    continue;
                }
                if (!head.isSealed()) {
                    head.seal(producerId, q.nextSequence, clock.now());
                    if (producerId >= 0) {
                        q.nextSequence += head.recordCount();
                    }
                }
                q.batches.pollFirst();
                q.inFlight++;
                inFlightBatches.incrementAndGet();
                ready.computeIfAbsent(leader, k -> new ArrayList<>()).add(head);
            }
        }
        for (Map.Entry<BrokerInfo, List<ProducerBatch>> e : ready.entrySet()) {
            sendTo(e.getKey(), e.getValue());
        }
        if (needMetadata) {
            metadata.refresh();
            nextWake = Math.min(nextWake, config.retryBackoffMs());
        }
        return nextWake;
    }

    private void sendTo(BrokerInfo leader, List<ProducerBatch> batches) {
        List<ProduceRequest.PartitionData> parts = new ArrayList<>(batches.size());
        for (ProducerBatch b : batches) {
            parts.add(new ProduceRequest.PartitionData(b.tp.topic(), b.tp.partition(),
                    MemoryRecords.of(List.of(b.sealedBatch()))));
        }
        ProduceRequest req = new ProduceRequest(config.acks(), (int) config.requestTimeoutMs(), parts);
        network.send(leader.address(), req, ProduceResponse::read).whenComplete((resp, err) -> {
            if (err != null) {
                metadata.refresh();
                for (ProducerBatch b : batches) {
                    retryOrFail(b, err instanceof NetworkException ? err : new NetworkException(err.toString(), err));
                }
                return;
            }
            if (resp == null) {
                for (ProducerBatch b : batches) {
                    b.complete(-1, -1);
                    done(b);
                }
                return;
            }
            Map<TopicPartition, ProduceResponse.PartitionResult> byTp = new HashMap<>();
            for (ProduceResponse.PartitionResult r : resp.partitions()) {
                byTp.put(new TopicPartition(r.topic(), r.partition()), r);
            }
            for (ProducerBatch b : batches) {
                ProduceResponse.PartitionResult r = byTp.get(b.tp);
                Errors error = r == null ? Errors.UNKNOWN : r.error();
                if (error == Errors.NONE) {
                    clock.observe(r.logAppendTime());
                    b.complete(r.baseOffset(), r.logAppendTime());
                    done(b);
                } else if (error.retriable()) {
                    if (error == Errors.NOT_LEADER_OR_FOLLOWER || error == Errors.UNKNOWN_TOPIC_OR_PARTITION
                            || error == Errors.FENCED_LEADER_EPOCH) {
                        metadata.refresh();
                    }
                    retryOrFail(b, error.exception("producing to " + b.tp));
                } else {
                    if (error == Errors.OUT_OF_ORDER_SEQUENCE_NUMBER || error == Errors.UNKNOWN_PRODUCER_ID) {
                        resetProducerId = true;
                    }
                    finishInFlight(b, error.exception("producing to " + b.tp));
                }
            }
        });
    }

    private void retryOrFail(ProducerBatch b, Throwable cause) {
        long now = System.currentTimeMillis();
        if (b.attempts >= config.retries() || now >= b.deadlineMs || !running) {
            if (config.idempotence()) {
                // We can't tell whether the broker wrote it, so later sequences may now leave a gap.
                resetProducerId = true;
            }
            finishInFlight(b, cause);
            return;
        }
        b.attempts++;
        b.retryAtMs = now + config.retryBackoffMs();
        PartitionQueue q = queues.get(b.tp);
        synchronized (q) {
            q.batches.addFirst(b);
            q.inFlight--;
        }
        inFlightBatches.decrementAndGet();
        LockSupport.unpark(sender);
    }

    private void done(ProducerBatch b) {
        bufferBytes.release(b.bytes());
        PartitionQueue q = queues.get(b.tp);
        synchronized (q) {
            q.inFlight--;
        }
        inFlightBatches.decrementAndGet();
        LockSupport.unpark(sender);
    }

    private void finishInFlight(ProducerBatch b, Throwable t) {
        b.fail(t);
        done(b);
    }

    private void finish(ProducerBatch b, Throwable t) {
        b.fail(t);
        bufferBytes.release(b.bytes());
    }

    private boolean idle() {
        if (inFlightBatches.get() > 0) {
            return false;
        }
        for (PartitionQueue q : queues.values()) {
            synchronized (q) {
                if (!q.batches.isEmpty() || q.inFlight > 0) {
                    return false;
                }
            }
        }
        return true;
    }

    /** Sends everything buffered and waits for all outstanding sends to finish. */
    public void flush() throws InterruptedException {
        flushing = true;
        try {
            LockSupport.unpark(sender);
            while (!idle()) {
                Thread.sleep(1);
                LockSupport.unpark(sender);
            }
        } finally {
            flushing = false;
        }
    }

    public long producerId() {
        return producerId;
    }

    @Override
    public void close() {
        closing = true;
        try {
            long deadline = System.currentTimeMillis() + config.deliveryTimeoutMs();
            while (!idle() && System.currentTimeMillis() < deadline) {
                LockSupport.unpark(sender);
                Thread.sleep(1);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        running = false;
        LockSupport.unpark(sender);
        try {
            sender.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        for (PartitionQueue q : queues.values()) {
            synchronized (q) {
                for (ProducerBatch b : q.batches) {
                    b.fail(new IllegalStateException("producer closed"));
                }
                q.batches.clear();
            }
        }
        network.close();
    }
}
