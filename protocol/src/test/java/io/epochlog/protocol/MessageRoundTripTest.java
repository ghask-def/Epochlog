package io.epochlog.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import io.epochlog.common.RecordBatch;
import io.epochlog.common.RecordBatchBuilder;
import io.epochlog.protocol.message.AlterIsrRequest;
import io.epochlog.protocol.message.BrokerInfo;
import io.epochlog.protocol.message.CreateTopicRequest;
import io.epochlog.protocol.message.FetchRequest;
import io.epochlog.protocol.message.FetchResponse;
import io.epochlog.protocol.message.LeaderAndIsrRequest;
import io.epochlog.protocol.message.MetadataResponse;
import io.epochlog.protocol.message.PartitionState;
import io.epochlog.protocol.message.ProduceRequest;
import io.epochlog.protocol.message.TopicConfig;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class MessageRoundTripTest {

    private static <T> T roundTrip(java.util.function.Consumer<ProtocolWriter> write, Function<ProtocolReader, T> read) {
        ProtocolWriter w = new ProtocolWriter();
        write.accept(w);
        ByteBuffer buf = w.toByteBuffer();
        ProtocolReader r = new ProtocolReader(buf);
        T out = read.apply(r);
        assertThat(r.remaining()).isZero();
        return out;
    }

    @Test
    void produceRequestCarriesBatchesUntouched() {
        RecordBatch batch = new RecordBatchBuilder().producer(9, 0)
                .append("k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8)).build();
        ProduceRequest req = new ProduceRequest((short) -1, 5000,
                List.of(new ProduceRequest.PartitionData("orders", 3, Records.of(batch))));
        ProduceRequest back = roundTrip(req::write, ProduceRequest::read);
        assertThat(back.acks()).isEqualTo((short) -1);
        ProduceRequest.PartitionData p = back.partitions().get(0);
        assertThat(p.topic()).isEqualTo("orders");
        List<RecordBatch> batches = p.records().batches();
        assertThat(batches).hasSize(1);
        assertThat(batches.get(0).isValid()).isTrue();
        assertThat(batches.get(0).producerId()).isEqualTo(9);
    }

    @Test
    void fetchRoundTrip() {
        FetchRequest req = new FetchRequest(ApiKey.REPLICA_FETCH, 2, 7L, 500, 1, 1 << 20,
                List.of(new FetchRequest.PartitionData("t", 0, 42, 3, 4096)));
        FetchRequest back = roundTrip(req::write, r -> FetchRequest.read(ApiKey.REPLICA_FETCH, r));
        assertThat(back).isEqualTo(req);

        FetchResponse resp = new FetchResponse(List.of(new FetchResponse.PartitionData("t", 0, Errors.NONE, 10, 0, 12,
                MemoryRecords.EMPTY)));
        FetchResponse respBack = roundTrip(resp::write, FetchResponse::read);
        assertThat(respBack.partitions().get(0).highWatermark()).isEqualTo(10);
        assertThat(respBack.partitions().get(0).records().sizeInBytes()).isZero();
    }

    @Test
    void controlPlaneMessages() {
        LeaderAndIsrRequest lai = new LeaderAndIsrRequest(0, 17,
                List.of(new BrokerInfo(1, "a", 9092), new BrokerInfo(2, "b", 9093)),
                List.of(new PartitionState("t", 0, 1, 4, 9, List.of(1, 2, 3), List.of(1, 2))),
                List.of(new TopicConfig("t", Map.of("segment.bytes", "1024"))));
        LeaderAndIsrRequest laiBack = roundTrip(lai::write, LeaderAndIsrRequest::read);
        assertThat(laiBack).isEqualTo(lai);

        CreateTopicRequest ct = new CreateTopicRequest("t", 3, (short) 2, Map.of("cleanup.policy", "compact"));
        CreateTopicRequest ctBack = roundTrip(ct::write, CreateTopicRequest::read);
        assertThat(ctBack).isEqualTo(ct);

        AlterIsrRequest ai = new AlterIsrRequest(1, 5, List.of(new AlterIsrRequest.PartitionData("t", 0, 4, 9,
                List.of(1))));
        AlterIsrRequest aiBack = roundTrip(ai::write, AlterIsrRequest::read);
        assertThat(aiBack).isEqualTo(ai);

        MetadataResponse md = new MetadataResponse(List.of(new BrokerInfo(1, "a", 1)),
                List.of(new MetadataResponse.TopicMetadata("t", Errors.NONE,
                        List.of(new PartitionState("t", 0, 1, 0, 0, List.of(1), List.of(1))))));
        MetadataResponse mdBack = roundTrip(md::write, MetadataResponse::read);
        assertThat(mdBack).isEqualTo(md);
    }

    @Test
    void nullStringsAndBytes() {
        ProtocolReader r = new ProtocolReader(new ProtocolWriter().putString(null).putBytes(null).putString("é")
                .toByteBuffer());
        assertThat(r.getString()).isNull();
        assertThat(r.getBytes()).isNull();
        assertThat(r.getString()).isEqualTo("é");
    }
}
