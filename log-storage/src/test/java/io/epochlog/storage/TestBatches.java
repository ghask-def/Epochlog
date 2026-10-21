package io.epochlog.storage;

import io.epochlog.common.RecordBatch;
import io.epochlog.common.RecordBatchBuilder;
import java.nio.charset.StandardCharsets;

final class TestBatches {
    private TestBatches() {}

    static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    static RecordBatch batch(String... values) {
        RecordBatchBuilder b = new RecordBatchBuilder();
        for (String v : values) {
            b.append(null, bytes(v));
        }
        return b.build();
    }

    static RecordBatch keyed(String key, String value) {
        return new RecordBatchBuilder().append(bytes(key), bytes(value)).build();
    }

    static RecordBatch sized(int records, int valueSize) {
        RecordBatchBuilder b = new RecordBatchBuilder();
        for (int i = 0; i < records; i++) {
            b.append(null, new byte[valueSize]);
        }
        return b.build();
    }

    static RecordBatch idempotent(long pid, int seq, String... values) {
        RecordBatchBuilder b = new RecordBatchBuilder().producer(pid, seq);
        for (String v : values) {
            b.append(null, bytes(v));
        }
        return b.build();
    }
}
