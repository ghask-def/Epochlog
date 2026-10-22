package io.epochlog.broker;

import static org.assertj.core.api.Assertions.assertThat;

import io.epochlog.common.Config;
import io.epochlog.common.HybridTime;
import io.epochlog.common.RecordBatchBuilder;
import io.epochlog.common.Time;
import io.epochlog.network.NetworkClient;
import io.epochlog.protocol.Errors;
import io.epochlog.protocol.MemoryRecords;
import io.epochlog.protocol.message.CreateTopicRequest;
import io.epochlog.protocol.message.ListOffsetsRequest;
import io.epochlog.protocol.message.ListOffsetsResponse;
import io.epochlog.protocol.message.ProduceRequest;
import io.epochlog.protocol.message.ProduceResponse;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Drives a broker whose wall clock we control, to check HLC behaviour under skew. */
class ClockSkewTest {
    @TempDir
    Path dir;

    private final AtomicLong brokerWall = new AtomicLong(1_700_000_000_000L);
    private Broker broker;
    private NetworkClient client;

    private void start() throws Exception {
        broker = new Broker(BrokerConfig.from(Config.empty()
                .with("broker.id", 1).with("host", "127.0.0.1").with("advertised.host", "127.0.0.1")
                .with("port", 0).with("data.dir", dir.toString()).with("hlc.max.drift.ms", 500)),
                Time.SYSTEM, brokerWall::get);
        broker.start();
        if (client == null) {
            client = new NetworkClient("test", "skew-test", 5_000);
        }
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
        if (broker != null) {
            broker.close();
        }
    }

    private InetSocketAddress addr() {
        return new InetSocketAddress("127.0.0.1", broker.port());
    }

    /** Produces one batch stamped with the producer's own idea of the time; returns the broker's stamp. */
    private long produce(long producerHlc, String... values) throws Exception {
        RecordBatchBuilder b = new RecordBatchBuilder().baseTimestamp(producerHlc);
        for (String v : values) {
            b.append(null, v.getBytes(StandardCharsets.UTF_8));
        }
        ProduceRequest req = new ProduceRequest((short) 1, 5_000, List.of(
                new ProduceRequest.PartitionData("t", 0, MemoryRecords.of(List.of(b.build())))));
        ProduceResponse resp = client.send(addr(), req, ProduceResponse::read).get(5, TimeUnit.SECONDS);
        assertThat(resp.partitions().get(0).error()).isEqualTo(Errors.NONE);
        return resp.partitions().get(0).logAppendTime();
    }

    private void createTopic() throws Exception {
        broker.createTopic(new CreateTopicRequest("t", 1, (short) 1, Map.of())).get();
    }

    @Test
    void timestampsStayMonotonicWhenWallClockStepsBackAcrossRestart() throws Exception {
        start();
        createTopic();
        List<Long> stamps = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            stamps.add(produce(-1, "a" + i, "b" + i));
            brokerWall.addAndGet(5);
        }
        broker.close();

        // NTP yanks the clock back ten seconds while the broker is down.
        brokerWall.addAndGet(-10_000);
        start();
        for (int i = 0; i < 20; i++) {
            stamps.add(produce(-1, "c" + i));
        }
        for (int i = 1; i < stamps.size(); i++) {
            assertThat(stamps.get(i)).as("stamp %d", i).isGreaterThan(stamps.get(i - 1));
        }
    }

    @Test
    void producerAheadWithinDriftPullsBrokerClockForward() throws Exception {
        start();
        createTopic();
        long producerTime = HybridTime.fromMillis(brokerWall.get() + 300);
        long stamp = produce(producerTime, "x");
        // Causality: the record is stamped after the producer's own clock reading.
        assertThat(stamp).isGreaterThan(producerTime);
        assertThat(broker.clock().driftRejections()).isZero();
    }

    @Test
    void producerBeyondMaxDriftIsRejectedAndCounted() throws Exception {
        start();
        createTopic();
        long wayAhead = HybridTime.fromMillis(brokerWall.get() + 60_000);
        long stamp = produce(wayAhead, "y");
        assertThat(broker.clock().driftRejections()).isEqualTo(1);
        // The bad clock didn't drag the partition's timestamps an hour into the future.
        assertThat(HybridTime.physicalMillis(stamp)).isLessThan(brokerWall.get() + 1_000);
    }

    @Test
    void listOffsetsByTimestampUnderSkew() throws Exception {
        start();
        createTopic();
        List<Long> stamps = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            stamps.add(produce(-1, "r" + i));
            brokerWall.addAndGet(i == 4 ? -2_000 : 100);
        }
        for (int i = 0; i < 10; i++) {
            ListOffsetsResponse resp = client.send(addr(),
                    new ListOffsetsRequest(List.of(new ListOffsetsRequest.PartitionData("t", 0, stamps.get(i)))),
                    ListOffsetsResponse::read).get(5, TimeUnit.SECONDS);
            assertThat(resp.partitions().get(0).offset()).isEqualTo(i);
        }
    }
}
