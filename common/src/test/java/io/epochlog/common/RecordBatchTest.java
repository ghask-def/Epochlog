package io.epochlog.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class RecordBatchTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void roundTripsRecords(CompressionType compression) {
        RecordBatch batch = new RecordBatchBuilder(compression, 100, 64)
                .producer(7L, 3)
                .leaderEpoch(2)
                .baseTimestamp(HybridTime.pack(1_000, 0))
                .append(b("k1"), b("v1"))
                .append(null, b("v2"), List.of(Header.of("trace", "abc")))
                .append(b("k3"), null)
                .build();

        batch.ensureValid();
        assertThat(batch.baseOffset()).isEqualTo(100);
        assertThat(batch.lastOffset()).isEqualTo(102);
        assertThat(batch.recordCount()).isEqualTo(3);
        assertThat(batch.producerId()).isEqualTo(7L);
        assertThat(batch.baseSequence()).isEqualTo(3);
        assertThat(batch.lastSequence()).isEqualTo(5);
        assertThat(batch.leaderEpoch()).isEqualTo(2);
        assertThat(batch.compression()).isEqualTo(compression);

        List<Record> records = batch.records();
        assertThat(records).hasSize(3);
        assertThat(records.get(0).offset()).isEqualTo(100);
        assertThat(records.get(0).key()).isEqualTo(b("k1"));
        assertThat(records.get(1).key()).isNull();
        assertThat(records.get(1).headers()).containsExactly(Header.of("trace", "abc"));
        assertThat(records.get(2).value()).isNull();
        assertThat(records.get(2).timestamp()).isEqualTo(HybridTime.pack(1_000, 2));
    }

    @Test
    void baseOffsetAndEpochAreOutsideTheCrc() {
        RecordBatch batch = new RecordBatchBuilder().append(b("k"), b("v")).build();
        batch.setBaseOffset(42);
        batch.setLeaderEpoch(9);
        assertThat(batch.isValid()).isTrue();
        assertThat(batch.records().get(0).offset()).isEqualTo(42);
    }

    @Test
    void restampingTimestampKeepsCrcValid() {
        RecordBatch batch = new RecordBatchBuilder().append(b("a"), b("1")).append(b("b"), b("2")).build();
        long ts = HybridTime.pack(5_000, 10);
        batch.setBaseTimestamp(ts);
        assertThat(batch.isValid()).isTrue();
        assertThat(batch.maxTimestamp()).isEqualTo(ts + 1);
        assertThat(batch.records()).extracting(Record::timestamp).containsExactly(ts, ts + 1);
    }

    @Test
    void detectsFlippedBit() {
        RecordBatch batch = new RecordBatchBuilder().append(b("k"), b("value")).build();
        ByteBuffer raw = batch.buffer();
        int pos = raw.limit() - 2;
        raw.put(pos, (byte) (raw.get(pos) ^ 0x10));
        assertThat(batch.isValid()).isFalse();
        assertThatThrownBy(batch::ensureValid).isInstanceOf(CorruptRecordException.class);
    }

    @Test
    void readAllStopsAtPartialBatch() {
        RecordBatch a = new RecordBatchBuilder(CompressionType.NONE, 0, 64).append(b("a"), b("1")).build();
        RecordBatch c = new RecordBatchBuilder(CompressionType.NONE, 1, 64).append(b("c"), b("2")).build();
        ByteBuffer buf = ByteBuffer.allocate(a.sizeInBytes() + c.sizeInBytes() - 5);
        buf.put(a.buffer());
        ByteBuffer partial = c.buffer();
        partial.limit(partial.limit() - 5);
        buf.put(partial);
        buf.flip();
        List<RecordBatch> batches = RecordBatch.readAll(buf);
        assertThat(batches).hasSize(1);
        assertThat(batches.get(0).baseOffset()).isZero();
    }

    @Test
    void compactedBatchKeepsOffsetSpan() {
        RecordBatch batch = new RecordBatchBuilder(CompressionType.NONE, 10, 64)
                .producer(1, 0)
                .appendWithOffset(12, b("k"), b("v"), List.of())
                .lastOffset(15)
                .build();
        assertThat(batch.lastOffset()).isEqualTo(15);
        assertThat(batch.recordCount()).isEqualTo(1);
        assertThat(batch.records().get(0).offset()).isEqualTo(12);
        batch.ensureValid();
    }

    @Test
    void varintsRoundTrip() {
        ByteBuffer buf = ByteBuffer.allocate(64);
        int[] ints = {0, 1, -1, 63, -64, Integer.MAX_VALUE, Integer.MIN_VALUE};
        long[] longs = {0L, Long.MAX_VALUE, Long.MIN_VALUE, -12345678901L};
        for (int i : ints) {
            buf.clear();
            ByteUtils.writeVarint(i, buf);
            assertThat(buf.position()).isEqualTo(ByteUtils.sizeOfVarint(i));
            buf.flip();
            assertThat(ByteUtils.readVarint(buf)).isEqualTo(i);
        }
        for (long l : longs) {
            buf.clear();
            ByteUtils.writeVarlong(l, buf);
            assertThat(buf.position()).isEqualTo(ByteUtils.sizeOfVarlong(l));
            buf.flip();
            assertThat(ByteUtils.readVarlong(buf)).isEqualTo(l);
        }
    }
}
