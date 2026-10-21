package io.epochlog.common;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A view over one serialized record batch. Batches are the unit of write, replication and fsync.
 *
 * <pre>
 *  0  baseOffset       int64
 *  8  batchLength      int32   bytes following this field
 * 12  leaderEpoch      int32
 * 16  crc              int32   CRC32C of bytes [20, end)
 * 20  attributes       int16   low 3 bits: compression
 * 22  lastOffsetDelta  int32
 * 26  baseTimestamp    int64   HLC
 * 34  maxTimestamp     int64   HLC
 * 42  producerId       int64
 * 50  baseSequence     int32
 * 54  recordCount      int32
 * 58  records...
 * </pre>
 *
 * <p>The base offset and leader epoch sit outside the CRC so a leader can stamp them without
 * re-checksumming. Timestamps are covered, so re-stamping them recomputes the CRC.
 *
 * <p>Thread-safety: not thread-safe. The accessors are safe to call concurrently only if nobody
 * calls a setter.
 */
public final class RecordBatch {
    public static final int BASE_OFFSET_OFFSET = 0;
    public static final int LENGTH_OFFSET = 8;
    public static final int LEADER_EPOCH_OFFSET = 12;
    public static final int CRC_OFFSET = 16;
    public static final int ATTRIBUTES_OFFSET = 20;
    public static final int LAST_OFFSET_DELTA_OFFSET = 22;
    public static final int BASE_TIMESTAMP_OFFSET = 26;
    public static final int MAX_TIMESTAMP_OFFSET = 34;
    public static final int PRODUCER_ID_OFFSET = 42;
    public static final int BASE_SEQUENCE_OFFSET = 50;
    public static final int RECORD_COUNT_OFFSET = 54;
    public static final int HEADER_SIZE = 58;

    /** Bytes before {@code batchLength} counts, i.e. base offset plus the length field itself. */
    public static final int LOG_OVERHEAD = 12;

    public static final long NO_PRODUCER_ID = -1L;
    public static final int NO_SEQUENCE = -1;
    public static final int NO_PARTITION_LEADER_EPOCH = -1;

    private static final int COMPRESSION_MASK = 0x07;

    private final ByteBuffer buffer;

    private RecordBatch(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    /**
     * Wraps the batch starting at the buffer's position. The buffer must contain at least the
     * whole batch; trailing bytes are ignored. Position of the argument is not changed.
     */
    public static RecordBatch wrap(ByteBuffer buf) {
        if (buf.remaining() < HEADER_SIZE) {
            throw new CorruptRecordException("buffer too small for batch header: " + buf.remaining());
        }
        int size = sizeAt(buf, buf.position());
        if (size < HEADER_SIZE || size > buf.remaining()) {
            throw new CorruptRecordException("bad batch size " + size + ", remaining " + buf.remaining());
        }
        ByteBuffer slice = buf.slice(buf.position(), size).order(ByteOrder.BIG_ENDIAN);
        return new RecordBatch(slice);
    }

    /** Total size of the batch whose header starts at {@code pos}, or -1 if it can't be read. */
    public static int sizeAt(ByteBuffer buf, int pos) {
        if (buf.limit() - pos < LOG_OVERHEAD) {
            return -1;
        }
        return buf.getInt(pos + LENGTH_OFFSET) + LOG_OVERHEAD;
    }

    /** Splits a buffer containing back-to-back batches. A trailing partial batch is dropped. */
    public static List<RecordBatch> readAll(ByteBuffer buf) {
        List<RecordBatch> out = new ArrayList<>();
        int pos = buf.position();
        while (pos < buf.limit()) {
            int size = sizeAt(buf, pos);
            if (size < HEADER_SIZE || pos + size > buf.limit()) {
                break;
            }
            out.add(new RecordBatch(buf.slice(pos, size).order(ByteOrder.BIG_ENDIAN)));
            pos += size;
        }
        return out;
    }

    public long baseOffset() {
        return buffer.getLong(BASE_OFFSET_OFFSET);
    }

    public void setBaseOffset(long offset) {
        buffer.putLong(BASE_OFFSET_OFFSET, offset);
    }

    public long lastOffset() {
        return baseOffset() + lastOffsetDelta();
    }

    public long nextOffset() {
        return lastOffset() + 1;
    }

    public int lastOffsetDelta() {
        return buffer.getInt(LAST_OFFSET_DELTA_OFFSET);
    }

    public int leaderEpoch() {
        return buffer.getInt(LEADER_EPOCH_OFFSET);
    }

    public void setLeaderEpoch(int epoch) {
        buffer.putInt(LEADER_EPOCH_OFFSET, epoch);
    }

    public int sizeInBytes() {
        return buffer.limit();
    }

    public int storedCrc() {
        return buffer.getInt(CRC_OFFSET);
    }

    public int computeCrc() {
        return Crc32c.compute(buffer, ATTRIBUTES_OFFSET, buffer.limit() - ATTRIBUTES_OFFSET);
    }

    public boolean isValid() {
        return sizeInBytes() >= HEADER_SIZE && storedCrc() == computeCrc();
    }

    public void ensureValid() {
        if (sizeInBytes() < HEADER_SIZE) {
            throw new CorruptRecordException("batch smaller than header: " + sizeInBytes());
        }
        int stored = storedCrc();
        int computed = computeCrc();
        if (stored != computed) {
            throw new CorruptRecordException(String.format(
                    "crc mismatch at base offset %d: stored %08x, computed %08x", baseOffset(), stored, computed));
        }
        if (lastOffsetDelta() < 0 || recordCount() < 0 || recordCount() > lastOffsetDelta() + 1) {
            throw new CorruptRecordException("inconsistent record count " + recordCount()
                    + " for last offset delta " + lastOffsetDelta());
        }
    }

    public CompressionType compression() {
        return CompressionType.forId(buffer.getShort(ATTRIBUTES_OFFSET) & COMPRESSION_MASK);
    }

    public long baseTimestamp() {
        return buffer.getLong(BASE_TIMESTAMP_OFFSET);
    }

    public long maxTimestamp() {
        return buffer.getLong(MAX_TIMESTAMP_OFFSET);
    }

    /**
     * Stamps a new base timestamp. Records carry timestamp deltas equal to their offset delta,
     * so the batch spans {@code [base, base + lastOffsetDelta]}. Recomputes the CRC.
     */
    public void setBaseTimestamp(long base) {
        buffer.putLong(BASE_TIMESTAMP_OFFSET, base);
        buffer.putLong(MAX_TIMESTAMP_OFFSET, base + lastOffsetDelta());
        buffer.putInt(CRC_OFFSET, computeCrc());
    }

    public long producerId() {
        return buffer.getLong(PRODUCER_ID_OFFSET);
    }

    public boolean hasProducerId() {
        return producerId() >= 0;
    }

    public int baseSequence() {
        return buffer.getInt(BASE_SEQUENCE_OFFSET);
    }

    public int lastSequence() {
        int base = baseSequence();
        return base == NO_SEQUENCE ? NO_SEQUENCE : base + lastOffsetDelta();
    }

    public int recordCount() {
        return buffer.getInt(RECORD_COUNT_OFFSET);
    }

    /** A read-only-position duplicate covering exactly this batch. */
    public ByteBuffer buffer() {
        return buffer.duplicate();
    }

    public List<Record> records() {
        int count = recordCount();
        if (count == 0) {
            return Collections.emptyList();
        }
        ByteBuffer body = buffer.duplicate().position(HEADER_SIZE);
        if (compression() != CompressionType.NONE) {
            byte[] packed = new byte[body.remaining()];
            body.get(packed);
            body = ByteBuffer.wrap(compression().decompress(packed));
        }
        long baseOffset = baseOffset();
        long baseTimestamp = baseTimestamp();
        List<Record> out = new ArrayList<>(count);
        try {
            for (int i = 0; i < count; i++) {
                out.add(readRecord(body, baseOffset, baseTimestamp));
            }
        } catch (RuntimeException e) {
            throw new CorruptRecordException("malformed records in batch at " + baseOffset, e);
        }
        return out;
    }

    private static Record readRecord(ByteBuffer in, long baseOffset, long baseTimestamp) {
        int length = ByteUtils.readVarint(in);
        int start = in.position();
        in.get(); // attributes, unused
        long tsDelta = ByteUtils.readVarlong(in);
        int offsetDelta = ByteUtils.readVarint(in);
        byte[] key = readBytes(in);
        byte[] value = readBytes(in);
        int headerCount = ByteUtils.readVarint(in);
        List<Header> headers = headerCount == 0 ? List.of() : new ArrayList<>(headerCount);
        for (int i = 0; i < headerCount; i++) {
            byte[] k = readBytes(in);
            headers.add(new Header(new String(k, StandardCharsets.UTF_8), readBytes(in)));
        }
        if (in.position() - start != length) {
            throw new CorruptRecordException("record length mismatch");
        }
        return new Record(baseOffset + offsetDelta, baseTimestamp + tsDelta, key, value, headers);
    }

    private static byte[] readBytes(ByteBuffer in) {
        int len = ByteUtils.readVarint(in);
        if (len < 0) {
            return null;
        }
        byte[] b = new byte[len];
        in.get(b);
        return b;
    }

    @Override
    public String toString() {
        return "RecordBatch(base=" + baseOffset() + ", last=" + lastOffset() + ", epoch=" + leaderEpoch()
                + ", records=" + recordCount() + ", size=" + sizeInBytes() + ")";
    }
}
