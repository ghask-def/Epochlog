package io.epochlog.bench;

import io.epochlog.broker.Broker;
import io.epochlog.broker.BrokerConfig;
import io.epochlog.broker.Partition;
import io.epochlog.client.Admin;
import io.epochlog.client.Producer;
import io.epochlog.client.ProducerRecord;
import io.epochlog.common.Config;
import io.epochlog.common.TopicPartition;
import io.epochlog.controller.Controller;
import io.epochlog.controller.ControllerConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

/**
 * End-to-end produce benchmark. For each configuration it starts a controller and three
 * brokers in this JVM, creates a replicated topic, and runs two phases:
 *
 * <ul>
 *   <li>throughput: producers send as fast as they can, with a cap on records in flight;
 *   <li>latency: a fixed send rate well below saturation, so percentiles show the cost of the
 *       configuration rather than queueing.
 * </ul>
 *
 * Results print as a Markdown table. Arguments are {@code key=value}: {@code seconds},
 * {@code rate}, {@code producers}, {@code partitions}, {@code recordBytes}, {@code only}.
 *
 * <p>Thread-safety: not applicable (entry point).
 */
public final class LoadGenerator {

    record Scenario(String name, String acks, boolean groupCommit, String handlers) {}

    record Result(Scenario scenario, double throughput, double mbPerSec, long p50Micros, long p99Micros,
            long p999Micros) {}

    private final int seconds;
    private final int rate;
    private final int producers;
    private final int partitions;
    private final int recordBytes;

    LoadGenerator(Map<String, String> args) {
        this.seconds = Integer.parseInt(args.getOrDefault("seconds", "10"));
        this.rate = Integer.parseInt(args.getOrDefault("rate", "20000"));
        this.producers = Integer.parseInt(args.getOrDefault("producers", "4"));
        this.partitions = Integer.parseInt(args.getOrDefault("partitions", "6"));
        this.recordBytes = Integer.parseInt(args.getOrDefault("recordBytes", "100"));
    }

    public static void main(String[] argv) throws Exception {
        Map<String, String> args = new HashMap<>();
        for (String a : argv) {
            int eq = a.indexOf('=');
            args.put(a.substring(0, eq), a.substring(eq + 1));
        }
        List<Scenario> scenarios = List.of(
                new Scenario("acks=1, group commit, virtual threads", "1", true, "virtual"),
                new Scenario("acks=all, group commit, virtual threads", "all", true, "virtual"),
                new Scenario("acks=1, no fsync, virtual threads", "1", false, "virtual"),
                new Scenario("acks=all, no fsync, virtual threads", "all", false, "virtual"),
                new Scenario("acks=1, group commit, 8 platform threads", "1", true, "8"),
                new Scenario("acks=all, group commit, 8 platform threads", "all", true, "8"));
        String only = args.get("only");
        LoadGenerator gen = new LoadGenerator(args);
        List<Result> results = new ArrayList<>();
        for (Scenario s : scenarios) {
            if (only != null && !s.name().contains(only)) {
                continue;
            }
            System.err.println("running: " + s.name());
            results.add(gen.run(s));
        }
        System.out.printf("%n%d producers, %d partitions (RF 3), %d-byte records, %ds per phase, latency phase at %,d msg/s%n%n",
                gen.producers, gen.partitions, gen.recordBytes, gen.seconds, gen.rate);
        System.out.println("| configuration | msgs/sec | MB/s | p50 | p99 | p99.9 |");
        System.out.println("|---|---:|---:|---:|---:|---:|");
        for (Result r : results) {
            System.out.printf("| %s | %,.0f | %.1f | %s | %s | %s |%n", r.scenario().name(), r.throughput(), r.mbPerSec(),
                    fmt(r.p50Micros()), fmt(r.p99Micros()), fmt(r.p999Micros()));
        }
    }

    private static String fmt(long micros) {
        return micros >= 10_000 ? String.format("%.1f ms", micros / 1000.0) : String.format("%.2f ms", micros / 1000.0);
    }

    Result run(Scenario s) throws Exception {
        Path dir = Files.createTempDirectory("epochlog-bench");
        Controller controller = new Controller(ControllerConfig.from(Config.empty()
                .with("host", "127.0.0.1").with("port", 0).with("data.dir", dir.resolve("c").toString())));
        controller.start();
        List<Broker> brokers = new ArrayList<>();
        try {
            StringBuilder bootstrap = new StringBuilder();
            for (int id = 1; id <= 3; id++) {
                Broker b = new Broker(BrokerConfig.from(Config.empty()
                        .with("broker.id", id).with("host", "127.0.0.1").with("advertised.host", "127.0.0.1")
                        .with("port", 0).with("data.dir", dir.resolve("b" + id).toString())
                        .with("controller", "127.0.0.1:" + controller.port())
                        .with("group.commit.enabled", s.groupCommit())
                        .with("handler.threads", s.handlers())
                        .with("min.insync.replicas", 2)
                        .with("replica.fetch.max.wait.ms", 50)));
                b.start();
                brokers.add(b);
                bootstrap.append(bootstrap.isEmpty() ? "" : ",").append("127.0.0.1:").append(b.port());
            }
            long deadline = System.currentTimeMillis() + 10_000;
            while (!(controller.isBrokerAlive(1) && controller.isBrokerAlive(2) && controller.isBrokerAlive(3))) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException("brokers did not register");
                }
                Thread.sleep(20);
            }
            try (Admin admin = new Admin(bootstrap.toString())) {
                admin.createTopic("bench", partitions, 3, Map.of());
            }
            Map<String, String> props = new HashMap<>();
            props.put("bootstrap.servers", bootstrap.toString());
            props.put("acks", s.acks());
            props.put("linger.ms", "5");
            props.put("batch.size", Integer.toString(128 * 1024));
            props.put("enable.idempotence", "true");

            phase(props, Integer.MAX_VALUE, 2, null);
            long[] throughputCount = new long[1];
            phase(props, Integer.MAX_VALUE, seconds, throughputCount);
            double throughput = throughputCount[0] / (double) seconds;
            awaitReplicasIdle(brokers);
            long[] latencies = phase(props, rate, seconds, null);
            Arrays.sort(latencies);
            return new Result(s, throughput, throughput * recordBytes / (1 << 20), percentile(latencies, 0.50),
                    percentile(latencies, 0.99), percentile(latencies, 0.999));
        } finally {
            for (Broker b : brokers) {
                b.close();
            }
            controller.close();
            try (Stream<Path> walk = Files.walk(dir)) {
                for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(p);
                }
            }
        }
    }

    /** Lets followers finish replicating the throughput phase so it doesn't skew the latency phase. */
    private static void awaitReplicasIdle(List<Broker> brokers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline) {
            Map<TopicPartition, Long> ends = new HashMap<>();
            boolean caughtUp = true;
            for (Broker b : brokers) {
                for (Partition p : b.replicaManager().allPartitions()) {
                    Long other = ends.putIfAbsent(p.topicPartition(), p.logEndOffset());
                    if (other != null && other != p.logEndOffset()) {
                        caughtUp = false;
                    }
                }
            }
            if (caughtUp) {
                Thread.sleep(1_000);
                return;
            }
            Thread.sleep(100);
        }
    }

    /**
     * Runs producers for {@code secs} seconds at {@code totalRate} records/s (MAX_VALUE for
     * unthrottled) and returns the per-record ack latencies in microseconds.
     */
    private long[] phase(Map<String, String> props, int totalRate, int secs, long[] countOut) throws Exception {
        int maxSamples = (int) Math.min(50_000_000L, (long) Math.min(totalRate, 2_000_000) * secs + 1_000);
        long[] samples = new long[totalRate == Integer.MAX_VALUE ? 0 : maxSamples];
        AtomicInteger sampleCount = new AtomicInteger();
        AtomicLong acked = new AtomicLong();
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(secs);
        byte[] payload = new byte[recordBytes];
        List<Thread> threads = new ArrayList<>();
        List<Producer> clients = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            Producer producer = new Producer(props);
            clients.add(producer);
            long intervalNanos = totalRate == Integer.MAX_VALUE ? 0 : TimeUnit.SECONDS.toNanos(1) * producers / totalRate;
            Semaphore inFlight = new Semaphore(50_000);
            threads.add(Thread.ofPlatform().start(() -> {
                long next = System.nanoTime();
                while (System.nanoTime() < end) {
                    if (intervalNanos > 0) {
                        long wait = next - System.nanoTime();
                        if (wait > 0) {
                            LockSupport.parkNanos(wait);
                        }
                        next += intervalNanos;
                    }
                    inFlight.acquireUninterruptibly();
                    long start = System.nanoTime();
                    producer.send(new ProducerRecord("bench", null, null, payload, null)).whenComplete((md, err) -> {
                        inFlight.release();
                        if (err != null) {
                            return;
                        }
                        if (System.nanoTime() <= end) {
                            acked.incrementAndGet();
                        }
                        if (samples.length > 0) {
                            int i = sampleCount.getAndIncrement();
                            if (i < samples.length) {
                                samples[i] = (System.nanoTime() - start) / 1_000;
                            }
                        }
                    });
                }
            }));
        }
        for (Thread t : threads) {
            t.join();
        }
        for (Producer p : clients) {
            p.flush();
            p.close();
        }
        if (countOut != null) {
            countOut[0] = acked.get();
        }
        return Arrays.copyOf(samples, Math.min(sampleCount.get(), samples.length));
    }

    private static long percentile(long[] sorted, double q) {
        if (sorted.length == 0) {
            return 0;
        }
        return sorted[(int) Math.min(sorted.length - 1, Math.floor(q * sorted.length))];
    }
}
