package io.epochlog.common;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Accumulates records and produces a {@link RecordBatch}. Each record's timestamp delta equals
 * its offset delta, so every record in the log ends up with a distinct HLC timestamp once the
 * leader stamps the batch.
 *
 * <p>Thread-safety: not thread-safe.
 */
public final class RecordBatchBuilder {
    private final CompressionType compression;
    private final long baseOffset;
    private ByteBuffer records;
    private int count;
    private int lastOffsetDelta = -1;
    private long producerId = RecordBatch.NO_PRODUCER_ID;
    private int baseSequence = RecordBatch.NO_SEQUENCE;
    private int leaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH;
    private long baseTimestamp = HybridTime.NO_TIMESTAMP;

    public RecordBatchBuilder(CompressionType compression, long baseOffset, int initialCapacity) {
        this.compression = compression;
        this.baseOffset = baseOffset;
        this.records = ByteBuffer.allocate(Math.max(64, initialCapacity));
    }

    public RecordBatchBuilder() {
        this(CompressionType.NONE, 0L, 1024);
    }

    public RecordBatchBuilder producer(long producerId, int baseSequence) {
        this.producerId = producerId;
        this.baseSequence = baseSequence;
        return this;
    }

    public RecordBatchBuilder leaderEpoch(int epoch) {
        this.leaderEpoch = epoch;
        return this;
    }

    public RecordBatchBuilder baseTimestamp(long ts) {
        this.baseTimestamp = ts;
        return this;
    }

    public RecordBatchBuilder append(byte[] key, byte[] value, List<Header> headers) {
        return appendWithOffsetDelta(lastOffsetDelta + 1, key, value, headers);
    }

    public RecordBatchBuilder append(byte[] key, byte[] value) {
        return append(key, value, List.of());
    }

    /** Appends a record at an explicit absolute offset. Used by compaction, which leaves gaps. */
    public RecordBatchBuilder appendWithOffset(long offset, byte[] key, byte[] value, List<Header> headers) {
        long delta = offset - baseOffset;
        if (delta <= lastOffsetDelta || delta > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("offset " + offset + " out of order for base " + baseOffset);
        }
        return appendWithOffsetDelta((int) delta, key, value, headers);
    }

    /** Extends the batch's offset range without adding records, e.g. to keep a compacted batch's span. */
    public RecordBatchBuilder lastOffset(long offset) {
        long delta = offset - baseOffset;
        if (delta < lastOffsetDelta) {
            throw new IllegalArgumentException("last offset moves backwards");
        }
        lastOffsetDelta = (int) delta;
        return this;
    }

    private RecordBatchBuilder appendWithOffsetDelta(int offsetDelta, byte[] key, byte[] value, List<Header> headers) {
        int bodySize = 1
                + ByteUtils.sizeOfVarlong(offsetDelta)
                + ByteUtils.sizeOfVarint(offsetDelta)
                + sizeOfBytes(key)
                + sizeOfBytes(value)
                + ByteUtils.sizeOfVarint(headers.size());
        for (Header h : headers) {
            bodySize += sizeOfBytes(h.key().getBytes(StandardCharsets.UTF_8)) + sizeOfBytes(h.value());
        }
        ensureCapacity(ByteUtils.sizeOfVarint(bodySize) + bodySize);
        ByteUtils.writeVarint(bodySize, records);
        records.put((byte) 0);
        ByteUtils.writeVarlong(offsetDelta, records);
        ByteUtils.writeVarint(offsetDelta, records);
        writeBytes(key);
        writeBytes(value);
        ByteUtils.writeVarint(headers.size(), records);
        for (Header h : headers) {
            writeBytes(h.key().getBytes(StandardCharsets.UTF_8));
            writeBytes(h.value());
        }
        count++;
        lastOffsetDelta = offsetDelta;
        return this;
    }

    public int recordCount() {
        return count;
    }

    public boolean isEmpty() {
        return count == 0;
    }

    /** Uncompressed size the batch would have if built now. */
    public int estimatedSizeInBytes() {
        return RecordBatch.HEADER_SIZE + records.position();
    }

    public static int estimateRecordSize(byte[] key, byte[] value, List<Header> headers) {
        int size = 16 + sizeOfBytes(key) + sizeOfBytes(value);
        for (Header h : headers) {
            size += h.key().length() + sizeOfBytes(h.value()) + 2;
        }
        return size;
    }

    public RecordBatch build() {
        if (lastOffsetDelta < 0) {
            throw new IllegalStateException("batch has no offsets");
        }
        byte[] body;
        int bodyLen;
        if (compression == CompressionType.NONE) {
            body = records.array();
            bodyLen = records.position();
        } else {
            body = compression.compress(records.array(), records.position());
            bodyLen = body.length;
        }
        ByteBuffer out = ByteBuffer.allocate(RecordBatch.HEADER_SIZE + bodyLen);
        out.putLong(baseOffset);
        out.putInt(RecordBatch.HEADER_SIZE + bodyLen - RecordBatch.LOG_OVERHEAD);
        out.putInt(leaderEpoch);
        out.putInt(0);
        out.putShort((short) compression.id);
        out.putInt(lastOffsetDelta);
        out.putLong(baseTimestamp);
        out.putLong(baseTimestamp == HybridTime.NO_TIMESTAMP ? baseTimestamp : baseTimestamp + lastOffsetDelta);
        out.putLong(producerId);
        out.putInt(baseSequence);
        out.putInt(count);
        out.put(body, 0, bodyLen);
        out.flip();
        out.putInt(RecordBatch.CRC_OFFSET, Crc32c.compute(out, RecordBatch.ATTRIBUTES_OFFSET, out.limit() - RecordBatch.ATTRIBUTES_OFFSET));
        return RecordBatch.wrap(out);
    }

    private void ensureCapacity(int extra) {
        if (records.remaining() >= extra) {
            return;
        }
        int newCap = Math.max(records.capacity() * 2, records.position() + extra);
        ByteBuffer bigger = ByteBuffer.allocate(newCap);
        records.flip();
        bigger.put(records);
        records = bigger;
    }

    private void writeBytes(byte[] b) {
        if (b == null) {
            ByteUtils.writeVarint(-1, records);
        } else {
            ByteUtils.writeVarint(b.length, records);
            records.put(b);
        }
    }

    private static int sizeOfBytes(byte[] b) {
        return b == null ? 1 : ByteUtils.sizeOfVarint(b.length) + b.length;
    }
}
