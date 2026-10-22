package io.epochlog.chaos;

import io.epochlog.client.Producer;
import io.epochlog.client.ProducerRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sends uniquely numbered records in the background and records which ones were acknowledged.
 *
 * <p>Thread-safety: {@link #start} and {@link #stop} from the test thread; sends run on a
 * dedicated thread.
 */
public final class Workload {
    private final Producer producer;
    private final String topic;
    private final String prefix;
    private final History history;
    private final long pauseMicros;
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final List<CompletableFuture<?>> pending = new ArrayList<>();
    private volatile boolean running;
    private Thread thread;

    public Workload(Producer producer, String topic, String prefix, History history, long pauseMicros) {
        this.producer = producer;
        this.topic = topic;
        this.prefix = prefix;
        this.history = history;
        this.pauseMicros = pauseMicros;
    }

    public Workload start() {
        running = true;
        thread = Thread.ofPlatform().name("workload-" + prefix).daemon().start(() -> {
            long i = 0;
            while (running) {
                String value = prefix + "-" + i++;
                CompletableFuture<?> f = producer.send(ProducerRecord.of(topic, value, value)).whenComplete((md, err) -> {
                    if (err == null) {
                        history.acked(value);
                    } else {
                        failed.incrementAndGet();
                    }
                });
                synchronized (pending) {
                    pending.add(f);
                }
                sent.incrementAndGet();
                if (pauseMicros > 0) {
                    java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(pauseMicros));
                }
            }
        });
        return this;
    }

    /** Stops sending and waits for every outstanding send to succeed or fail. */
    public void stop() throws Exception {
        running = false;
        thread.join();
        producer.flush();
        List<CompletableFuture<?>> all;
        synchronized (pending) {
            all = new ArrayList<>(pending);
        }
        for (CompletableFuture<?> f : all) {
            f.handle((v, e) -> null).get(120, TimeUnit.SECONDS);
        }
        producer.close();
    }

    public long sent() {
        return sent.get();
    }

    public long failed() {
        return failed.get();
    }
}
