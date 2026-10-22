package io.epochlog.broker;

import static org.assertj.core.api.Assertions.assertThat;

import io.epochlog.client.Admin;
import io.epochlog.client.Consumer;
import io.epochlog.client.ConsumerRecord;
import io.epochlog.client.Producer;
import io.epochlog.client.ProducerRecord;
import io.epochlog.common.Config;
import io.epochlog.common.RecordBatch;
import io.epochlog.common.RecordBatchBuilder;
import io.epochlog.common.TopicPartition;
import io.epochlog.network.NetworkClient;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.MemoryRecords;
import io.epochlog.protocol.message.InitProducerIdRequest;
import io.epochlog.protocol.message.InitProducerIdResponse;
import io.epochlog.protocol.message.ProduceRequest;
import io.epochlog.protocol.message.ProduceResponse;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IdempotenceAndCompactionTest {
    @TempDir
    Path dir;

    private Broker broker;
    private NetworkClient client;

    @BeforeEach
    void start() throws Exception {
        broker = new Broker(BrokerConfig.from(Config.empty()
                .with("broker.id", 1).with("host", "127.0.0.1").with("advertised.host", "127.0.0.1")
                .with("port", 0).with("data.dir", dir.toString())
                .with("log.cleaner.interval.ms", 200)));
        broker.start();
        client = new NetworkClient("test", "idempotence-test", 5_000);
    }

    @AfterEach
    void stop() {
        client.close();
        broker.close();
    }

    private InetSocketAddress addr() {
        return new InetSocketAddress("127.0.0.1", broker.port());
    }

    private String bootstrap() {
        return "127.0.0.1:" + broker.port();
    }

    private ProduceResponse.PartitionResult send(RecordBatch batch) throws Exception {
        ProduceRequest req = new ProduceRequest((short) 1, 5_000, List.of(
                new ProduceRequest.PartitionData("idem", 0, MemoryRecords.of(List.of(batch)))));
        return client.send(addr(), req, ProduceResponse::read).get(5, TimeUnit.SECONDS).partitions().get(0);
    }

    private static RecordBatch batch(long pid, int seq, String... values) {
        RecordBatchBuilder b = new RecordBatchBuilder().producer(pid, seq);
        for (String v : values) {
            b.append(null, v.getBytes());
        }
        return b.build();
    }

    @Test
    void retriedBatchesAreWrittenOnce() throws Exception {
        try (Admin admin = new Admin(bootstrap())) {
            admin.createTopic("idem", 1, 1, Map.of());
        }
        long pid = client.send(addr(), new InitProducerIdRequest(), InitProducerIdResponse::read)
                .get(5, TimeUnit.SECONDS).producerId();

        ProduceResponse.PartitionResult first = send(batch(pid, 0, "a", "b"));
        ProduceResponse.PartitionResult retry = send(batch(pid, 0, "a", "b"));
        assertThat(first.error()).isEqualTo(Errors.NONE);
        assertThat(retry.error()).isEqualTo(Errors.NONE);
        assertThat(retry.baseOffset()).isEqualTo(first.baseOffset());

        assertThat(send(batch(pid, 2, "c")).baseOffset()).isEqualTo(2);
        assertThat(send(batch(pid, 7, "gap")).error()).isEqualTo(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER);
        assertThat(send(batch(pid + 1, 4, "stranger")).error()).isEqualTo(Errors.UNKNOWN_PRODUCER_ID);

        Partition p = broker.replicaManager().partition(new TopicPartition("idem", 0)).orElseThrow();
        assertThat(p.logEndOffset()).isEqualTo(3);
    }

    @Test
    void producerIdsAreUniqueAcrossRestarts() throws Exception {
        long a = client.send(addr(), new InitProducerIdRequest(), InitProducerIdResponse::read).get().producerId();
        broker.close();
        broker = new Broker(BrokerConfig.from(Config.empty()
                .with("broker.id", 1).with("host", "127.0.0.1").with("advertised.host", "127.0.0.1")
                .with("port", 0).with("data.dir", dir.toString())));
        broker.start();
        long b = client.send(addr(), new InitProducerIdRequest(), InitProducerIdResponse::read).get().producerId();
        assertThat(b).isNotEqualTo(a);
    }

    @Test
    void compactedTopicKeepsLatestValuePerKey() throws Exception {
        try (Admin admin = new Admin(bootstrap())) {
            admin.createTopic("profiles", 1, 1, Map.of("cleanup.policy", "compact", "segment.bytes", "4096"));
        }
        Map<String, String> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrap());
        props.put("linger.ms", "0");
        // Small batches so most data lands in closed segments; the active one is never compacted.
        props.put("batch.size", "512");
        Map<String, String> latest = new HashMap<>();
        try (Producer p = new Producer(props)) {
            for (int i = 0; i < 2_000; i++) {
                String key = "user-" + (i % 20);
                String value = "v" + i;
                latest.put(key, value);
                p.send(ProducerRecord.of("profiles", key, value));
            }
            p.flush();
        }
        Partition partition = broker.replicaManager().partition(new TopicPartition("profiles", 0)).orElseThrow();
        long sizeBefore = partition.log().size();
        long deadline = System.currentTimeMillis() + 15_000;
        while (partition.log().size() > sizeBefore / 3 && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
        assertThat(partition.log().size()).isLessThan(sizeBefore / 3);
        assertThat(partition.logEndOffset()).isEqualTo(2_000);

        Map<String, String> seen = new HashMap<>();
        int count = 0;
        try (Consumer c = new Consumer(props)) {
            c.assignTopic("profiles");
            long last = -1;
            long end = System.currentTimeMillis() + 10_000;
            while (last < 1_999 && System.currentTimeMillis() < end) {
                for (ConsumerRecord r : c.poll(Duration.ofMillis(200))) {
                    assertThat(r.offset()).isGreaterThan(last);
                    last = r.offset();
                    seen.put(r.keyString(), r.valueString());
                    count++;
                }
            }
        }
        assertThat(seen).isEqualTo(latest);
        assertThat(count).isLessThan(2_000);
    }
}
