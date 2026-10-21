package io.epochlog.protocol;

import io.epochlog.common.RecordBatch;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Batches held in a heap buffer, as received off the wire or built by a producer.
 *
 * <p>Thread-safety: immutable as long as nobody writes through {@link #buffer()}.
 */
public record MemoryRecords(ByteBuffer buffer) implements Records {
    public static final MemoryRecords EMPTY = new MemoryRecords(ByteBuffer.allocate(0));

    public static MemoryRecords of(List<RecordBatch> batches) {
        int size = 0;
        for (RecordBatch b : batches) {
            size += b.sizeInBytes();
        }
        ByteBuffer buf = ByteBuffer.allocate(size);
        for (RecordBatch b : batches) {
            buf.put(b.buffer());
        }
        return new MemoryRecords(buf.flip());
    }

    @Override
    public int sizeInBytes() {
        return buffer.remaining();
    }

    /** Parses the batches. A trailing partial batch (possible in fetch responses) is dropped. */
    public List<RecordBatch> batches() {
        return RecordBatch.readAll(buffer.duplicate());
    }
}
