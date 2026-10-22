package io.epochlog.client;

import static org.assertj.core.api.Assertions.assertThat;

import io.epochlog.broker.Broker;
import io.epochlog.broker.BrokerConfig;
import io.epochlog.common.Config;
import io.epochlog.common.HybridTime;
import io.epochlog.common.TopicPartition;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SingleBrokerIntegrationTest {
    @TempDir
    Path dir;

    private Broker broker;
    private String bootstrap;

    @BeforeEach
    void startBroker() throws Exception {
        broker = startBroker(dir);
        bootstrap = "127.0.0.1:" + broker.port();
    }

    private static Broker startBroker(Path dataDir) throws Exception {
        Broker b = new Broker(BrokerConfig.from(Config.empty()
                .with("broker.id", 1)
                .with("host", "127.0.0.1")
                .with("advertised.host", "127.0.0.1")
                .with("port", 0)
                .with("data.dir", dataDir.toString())
                .with("log.segment.bytes", 16 << 20)));
        b.start();
        return b;
    }

    @AfterEach
    void stopBroker() {
        if (broker != null) {
            broker.close();
        }
    }

    private Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        m.put("bootstrap.servers", bootstrap);
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    private static byte[] payload(int partition, long seq) {
        return ByteBuffer.allocate(100).putInt(partition).putLong(seq).array();
    }

    @Test
    void producesAndConsumesOneMillionRecordsInOrder() throws Exception {
        int partitions = 4;
        int total = 1_000_000;
        try (Admin admin = new Admin(bootstrap)) {
            admin.createTopic("events", partitions, 1, Map.of());
        }
        try (Producer producer = new Producer(props("acks", "1", "linger.ms", "5", "batch.size", "131072"))) {
            List<CompletableFuture<RecordMetadata>> last = new ArrayList<>();
            for (int i = 0; i < total; i++) {
                int p = i % partitions;
                CompletableFuture<RecordMetadata> f = producer.send(ProducerRecord.of("events", p, null,
                        payload(p, i / partitions)));
                if (i >= total - partitions) {
                    last.add(f);
                }
            }
            producer.flush();
            for (CompletableFuture<RecordMetadata> f : last) {
                assertThat(f.get(10, TimeUnit.SECONDS).offset()).isEqualTo(total / partitions - 1);
            }
        }

        try (Consumer consumer = new Consumer(props("max.poll.records", "5000"))) {
            consumer.assignTopic("events");
            long[] next = new long[partitions];
            long[] lastTs = new long[partitions];
            int received = 0;
            long deadline = System.currentTimeMillis() + 120_000;
            while (received < total && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord r : consumer.poll(Duration.ofMillis(500))) {
                    ByteBuffer v = ByteBuffer.wrap(r.value());
                    int p = r.partition();
                    assertThat(v.getInt()).isEqualTo(p);
                    assertThat(v.getLong()).isEqualTo(next[p]);
                    assertThat(r.offset()).isEqualTo(next[p]);
                    assertThat(r.timestamp()).isGreaterThan(lastTs[p]);
                    lastTs[p] = r.timestamp();
                    next[p]++;
                    received++;
                }
            }
            assertThat(received).isEqualTo(total);
            assertThat(consumer.poll(Duration.ofMillis(200))).isEmpty();
        }
    }

    @Test
    void acksModesAndRestart() throws Exception {
        try (Admin admin = new Admin(bootstrap)) {
            admin.createTopic("acks", 1, 1, Map.of());
        }
        for (String acks : List.of("0", "1", "all")) {
            try (Producer producer = new Producer(props("acks", acks, "enable.idempotence",
                    acks.equals("0") ? "false" : "true"))) {
                for (int i = 0; i < 100; i++) {
                    producer.send(ProducerRecord.of("acks", "k", acks + "-" + i));
                }
                producer.flush();
            }
        }
        broker.close();
        broker = startBroker(dir);
        bootstrap = "127.0.0.1:" + broker.port();

        try (Consumer consumer = new Consumer(props())) {
            consumer.assignTopic("acks");
            List<String> values = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 20_000;
            while (values.size() < 300 && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord r : consumer.poll(Duration.ofMillis(200))) {
                    values.add(r.valueString());
                }
            }
            assertThat(values).hasSize(300);
            assertThat(values.get(0)).isEqualTo("0-0");
            assertThat(values.get(299)).isEqualTo("all-99");
        }
    }

    @Test
    void commitsOffsetsAndSeeksByTimestamp() throws Exception {
        try (Admin admin = new Admin(bootstrap)) {
            admin.createTopic("orders", 1, 1, Map.of());
        }
        TopicPartition tp = new TopicPartition("orders", 0);
        List<RecordMetadata> sent = new ArrayList<>();
        try (Producer producer = new Producer(props("linger.ms", "0"))) {
            for (int i = 0; i < 50; i++) {
                sent.add(producer.send(ProducerRecord.of("orders", "o" + i, "v" + i)).get(5, TimeUnit.SECONDS));
                if (i == 24) {
                    Thread.sleep(20);
                }
            }
        }
        for (int i = 1; i < sent.size(); i++) {
            assertThat(sent.get(i).timestamp()).isGreaterThan(sent.get(i - 1).timestamp());
        }

        try (Consumer consumer = new Consumer(props("group.id", "billing"))) {
            consumer.assign(List.of(tp));
            List<ConsumerRecord> got = new ArrayList<>();
            while (got.size() < 20) {
                got.addAll(consumer.poll(Duration.ofMillis(200)));
            }
            consumer.commitSync(Map.of(tp, 20L));
            assertThat(consumer.committed(tp)).hasValue(20L);

            long target = sent.get(25).timestamp();
            consumer.seekToTimestamp(tp, target);
            assertThat(consumer.position(tp)).isEqualTo(25);
            consumer.seekToTimestamp(tp, HybridTime.fromMillis(HybridTime.physicalMillis(target) + 3_600_000));
            assertThat(consumer.position(tp)).isEqualTo(50);
        }

        try (Consumer resumed = new Consumer(props("group.id", "billing"))) {
            resumed.assign(List.of(tp));
            List<ConsumerRecord> first = List.of();
            while (first.isEmpty()) {
                first = resumed.poll(Duration.ofMillis(200));
            }
            assertThat(first.get(0).offset()).isEqualTo(20);
            assertThat(first.get(0).valueString()).isEqualTo("v20");
        }
    }
}
