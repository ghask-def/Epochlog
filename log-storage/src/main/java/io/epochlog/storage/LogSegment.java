package io.epochlog.storage;

import io.epochlog.common.CorruptRecordException;
import io.epochlog.common.HybridTime;
import io.epochlog.common.Record;
import io.epochlog.common.RecordBatch;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;

/**
 * One segment of a partition log: {@code <base>.log} plus its offset and time indexes.
 *
 * <p>Thread-safety: single-writer. Appends, truncation and recovery come from the partition's
 * writer thread. Reads may run on any thread: each read snapshots the volatile {@code size},
 * which is written after the bytes and index entries it covers, then uses positional reads.
 */
public final class LogSegment implements Closeable {
    public static final String LOG_SUFFIX = ".log";
    public static final String INDEX_SUFFIX = ".index";
    public static final String TIME_INDEX_SUFFIX = ".timeindex";

    private final long baseOffset;
    private final LogConfig config;
    private final long createdMs;
    private Path logFile;
    private final FileChannel channel;
    private final OffsetIndex offsetIndex;
    private final TimeIndex timeIndex;

    private volatile int size;
    private volatile long nextOffset;
    private volatile long maxTimestamp = HybridTime.NO_TIMESTAMP;
    private int bytesSinceLastIndexEntry;

    private LogSegment(Path dir, long baseOffset, LogConfig config, long nowMs, boolean active, String suffix)
            throws IOException {
        this.baseOffset = baseOffset;
        this.config = config;
        this.logFile = dir.resolve(filenamePrefix(baseOffset) + LOG_SUFFIX + suffix);
        this.channel = FileChannel.open(logFile, StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        this.offsetIndex = new OffsetIndex(dir.resolve(filenamePrefix(baseOffset) + INDEX_SUFFIX + suffix),
                baseOffset, config.maxIndexBytes(), active);
        this.timeIndex = new TimeIndex(dir.resolve(filenamePrefix(baseOffset) + TIME_INDEX_SUFFIX + suffix),
                baseOffset, config.maxIndexBytes(), active);
        this.size = (int) channel.size();
        this.nextOffset = baseOffset;
        this.createdMs = nowMs;
        channel.position(size);
    }

    /** Opens or creates the segment. Call {@link #loadTail()} or {@link #recover} before use. */
    public static LogSegment open(Path dir, long baseOffset, LogConfig config, long nowMs, boolean active)
            throws IOException {
        return new LogSegment(dir, baseOffset, config, nowMs, active, "");
    }

    /** Opens a segment whose files carry an extra suffix, e.g. a compaction output. */
    static LogSegment openWithSuffix(Path dir, long baseOffset, LogConfig config, long nowMs, String suffix)
            throws IOException {
        return new LogSegment(dir, baseOffset, config, nowMs, true, suffix);
    }

    public static String filenamePrefix(long offset) {
        return String.format("%020d", offset);
    }

    public static long parseBaseOffset(Path file) {
        String name = file.getFileName().toString();
        return Long.parseLong(name.substring(0, name.indexOf('.')));
    }

    public long baseOffset() {
        return baseOffset;
    }

    public int size() {
        return size;
    }

    /** Offset after the last batch in this segment, or the base offset if empty. */
    public long nextOffset() {
        return nextOffset;
    }

    public long maxTimestamp() {
        return maxTimestamp;
    }

    public Path logFile() {
        return logFile;
    }

    OffsetIndex offsetIndex() {
        return offsetIndex;
    }

    TimeIndex timeIndex() {
        return timeIndex;
    }

    public boolean shouldRoll(int incomingBytes, long nowMs) {
        if (size == 0) {
            return false;
        }
        return (long) size + incomingBytes > config.segmentBytes()
                || nowMs - createdMs > config.segmentMs()
                || offsetIndex.isFull()
                || timeIndex.isFull()
                || nextOffset - baseOffset > Integer.MAX_VALUE - 1_000_000L;
    }

    /** Appends one batch whose offsets have already been assigned. */
    public void append(RecordBatch batch) throws IOException {
        int position = size;
        ByteBuffer buf = batch.buffer();
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
        indexBatch(batch.baseOffset(), batch.baseTimestamp(), position, batch.sizeInBytes());
        long ts = batch.maxTimestamp();
        if (ts > maxTimestamp) {
            maxTimestamp = ts;
        }
        nextOffset = batch.nextOffset();
        size = position + batch.sizeInBytes();
    }

    private void indexBatch(long batchBase, long batchTimestamp, int position, int batchSize) {
        if (position == 0 || bytesSinceLastIndexEntry >= config.indexIntervalBytes()) {
            if (offsetIndex.entries() == 0 || batchBase > offsetIndex.lastOffset()) {
                offsetIndex.append(batchBase, position);
                timeIndex.maybeAppend(batchTimestamp, batchBase);
            }
            bytesSinceLastIndexEntry = 0;
        }
        bytesSinceLastIndexEntry += batchSize;
    }

    /**
     * Returns whole batches starting with the one that contains {@code startOffset}, stopping
     * before any batch whose base offset is {@code >= maxOffset} or that would push the slice past
     * {@code maxBytes}. The first batch is always included, even if it alone is larger than
     * {@code maxBytes}, so a consumer can't get stuck behind an oversized batch. Returns null if
     * this segment has nothing at or after the offset below {@code maxOffset}.
     */
    public FileSlice read(long startOffset, int maxBytes, long maxOffset) throws IOException {
        int limit = size;
        int start = translateOffset(startOffset, limit);
        if (start < 0) {
            return null;
        }
        ByteBuffer header = ByteBuffer.allocate(RecordBatch.LOG_OVERHEAD);
        int pos = start;
        long firstBase = -1;
        while (pos < limit) {
            header.clear();
            FileSlice.readFully(channel, header, pos);
            long base = header.getLong(0);
            int batchSize = header.getInt(8) + RecordBatch.LOG_OVERHEAD;
            if (base >= maxOffset) {
                break;
            }
            int taken = pos - start;
            if (taken > 0 && taken + batchSize > maxBytes) {
                break;
            }
            if (firstBase < 0) {
                firstBase = base;
            }
            pos += batchSize;
        }
        if (pos == start) {
            return null;
        }
        return new FileSlice(channel, start, pos - start, firstBase);
    }

    /** Position of the first batch whose last offset is {@code >= offset}, or -1. */
    int translateOffset(long offset, int limit) throws IOException {
        OffsetIndex.OffsetPosition hint = offsetIndex.lookup(offset);
        ByteBuffer header = ByteBuffer.allocate(RecordBatch.LAST_OFFSET_DELTA_OFFSET + 4);
        int pos = hint.position();
        while (pos < limit) {
            header.clear();
            FileSlice.readFully(channel, header, pos);
            long last = header.getLong(0) + header.getInt(RecordBatch.LAST_OFFSET_DELTA_OFFSET);
            if (last >= offset) {
                return pos;
            }
            pos += header.getInt(8) + RecordBatch.LOG_OVERHEAD;
        }
        return -1;
    }

    /** First record with timestamp {@code >= target} at or after {@code minOffset}, or null. */
    public TimestampAndOffset findOffsetByTimestamp(long target, long minOffset) throws IOException {
        if (maxTimestamp < target) {
            return null;
        }
        int limit = size;
        TimeIndex.TimestampOffset hint = timeIndex.lookup(target);
        long from = Math.max(minOffset, hint.offset());
        int pos = translateOffset(from, limit);
        if (pos < 0) {
            return null;
        }
        ByteBuffer header = ByteBuffer.allocate(RecordBatch.HEADER_SIZE);
        while (pos < limit) {
            header.clear();
            FileSlice.readFully(channel, header, pos);
            int batchSize = header.getInt(RecordBatch.LENGTH_OFFSET) + RecordBatch.LOG_OVERHEAD;
            if (header.getLong(RecordBatch.MAX_TIMESTAMP_OFFSET) >= target) {
                ByteBuffer whole = ByteBuffer.allocate(batchSize);
                FileSlice.readFully(channel, whole, pos);
                for (Record r : RecordBatch.wrap(whole.flip()).records()) {
                    if (r.timestamp() >= target && r.offset() >= minOffset) {
                        return new TimestampAndOffset(r.timestamp(), r.offset());
                    }
                }
            }
            pos += batchSize;
        }
        return null;
    }

    /**
     * Rebuilds indexes by scanning every batch and validating its CRC. Truncates the file at the
     * first batch that is torn or corrupt.
     *
     * @return number of bytes truncated
     */
    public int recover(Consumer<RecordBatch> onValidBatch) throws IOException {
        offsetIndex.makeWritable(config.maxIndexBytes());
        timeIndex.makeWritable(config.maxIndexBytes());
        offsetIndex.reset();
        timeIndex.reset();
        bytesSinceLastIndexEntry = 0;
        maxTimestamp = HybridTime.NO_TIMESTAMP;
        long fileSize = channel.size();
        int pos = 0;
        long expectedMinOffset = baseOffset;
        ByteBuffer header = ByteBuffer.allocate(RecordBatch.LOG_OVERHEAD);
        while (pos + RecordBatch.HEADER_SIZE <= fileSize) {
            header.clear();
            FileSlice.readFully(channel, header, pos);
            long base = header.getLong(0);
            int batchSize = header.getInt(8) + RecordBatch.LOG_OVERHEAD;
            if (batchSize < RecordBatch.HEADER_SIZE || pos + (long) batchSize > fileSize || base < expectedMinOffset) {
                break;
            }
            ByteBuffer whole = ByteBuffer.allocate(batchSize);
            FileSlice.readFully(channel, whole, pos);
            RecordBatch batch = RecordBatch.wrap(whole.flip());
            try {
                batch.ensureValid();
            } catch (CorruptRecordException e) {
                break;
            }
            indexBatch(base, batch.baseTimestamp(), pos, batchSize);
            if (batch.maxTimestamp() > maxTimestamp) {
                maxTimestamp = batch.maxTimestamp();
            }
            onValidBatch.accept(batch);
            expectedMinOffset = batch.nextOffset();
            pos += batchSize;
        }
        int truncated = (int) (fileSize - pos);
        if (truncated > 0) {
            channel.truncate(pos);
        }
        channel.position(pos);
        nextOffset = expectedMinOffset;
        size = pos;
        return truncated;
    }

    /**
     * Loads end-of-segment state for a segment that was shut down cleanly. Scans from the last
     * index entry instead of the whole file.
     */
    public void loadTail() throws IOException {
        OffsetIndex.OffsetPosition last = offsetIndex.lastEntry();
        int pos = last.position();
        long next = baseOffset;
        long fileSize = channel.size();
        ByteBuffer header = ByteBuffer.allocate(RecordBatch.HEADER_SIZE);
        while (pos + RecordBatch.HEADER_SIZE <= fileSize) {
            header.clear();
            FileSlice.readFully(channel, header, pos);
            next = header.getLong(0) + header.getInt(RecordBatch.LAST_OFFSET_DELTA_OFFSET) + 1;
            pos += header.getInt(RecordBatch.LENGTH_OFFSET) + RecordBatch.LOG_OVERHEAD;
        }
        nextOffset = next;
        long lastTs = Math.max(timeIndex.lastTimestamp(), lastBatchMaxTimestamp());
        maxTimestamp = lastTs;
        bytesSinceLastIndexEntry = (int) (fileSize - last.position());
    }

    private long lastBatchMaxTimestamp() throws IOException {
        int limit = (int) channel.size();
        if (limit == 0) {
            return HybridTime.NO_TIMESTAMP;
        }
        long[] max = {HybridTime.NO_TIMESTAMP};
        forEachBatchHeader(offsetIndex.lastEntry().position(), h -> max[0] = Math.max(max[0], h.maxTimestamp()));
        return max[0];
    }

    /** Sanity-checks indexes after a clean open. */
    public boolean indexesLookSane() throws IOException {
        return offsetIndex.sanityCheck(channel.size());
    }

    /**
     * Visits a header-only view of each batch from {@code fromPosition} to the end. The view's
     * {@code records()} must not be called.
     */
    public void forEachBatchHeader(int fromPosition, Consumer<RecordBatch> visitor) throws IOException {
        int limit = size;
        int pos = fromPosition;
        while (pos + RecordBatch.HEADER_SIZE <= limit) {
            ByteBuffer header = ByteBuffer.allocate(RecordBatch.HEADER_SIZE);
            FileSlice.readFully(channel, header, pos);
            header.flip();
            int batchSize = header.getInt(RecordBatch.LENGTH_OFFSET) + RecordBatch.LOG_OVERHEAD;
            visitor.accept(headerView(header));
            pos += batchSize;
        }
    }

    private static RecordBatch headerView(ByteBuffer header) {
        // Fake the length so wrap() accepts a buffer holding only the header.
        ByteBuffer copy = ByteBuffer.allocate(RecordBatch.HEADER_SIZE);
        copy.put(header.duplicate()).flip();
        copy.putInt(RecordBatch.LENGTH_OFFSET, RecordBatch.HEADER_SIZE - RecordBatch.LOG_OVERHEAD);
        return RecordBatch.wrap(copy);
    }

    /**
     * Visitor for {@link #forEachBatch}.
     *
     * <p>Thread-safety: whatever the implementation provides.
     */
    @FunctionalInterface
    public interface BatchVisitor {
        void visit(RecordBatch batch) throws IOException;
    }

    /** Reads batches one at a time from the start of the segment. */
    public void forEachBatch(BatchVisitor visitor) throws IOException {
        int limit = size;
        int pos = 0;
        ByteBuffer header = ByteBuffer.allocate(RecordBatch.LOG_OVERHEAD);
        while (pos < limit) {
            header.clear();
            FileSlice.readFully(channel, header, pos);
            int batchSize = header.getInt(RecordBatch.LENGTH_OFFSET) + RecordBatch.LOG_OVERHEAD;
            ByteBuffer whole = ByteBuffer.allocate(batchSize);
            FileSlice.readFully(channel, whole, pos);
            visitor.visit(RecordBatch.wrap(whole.flip()));
            pos += batchSize;
        }
    }

    /** Reads every batch in the segment into memory. Intended for compaction and tests. */
    public java.util.List<RecordBatch> readAllBatches() throws IOException {
        int limit = size;
        if (limit == 0) {
            return java.util.List.of();
        }
        ByteBuffer all = ByteBuffer.allocate(limit);
        FileSlice.readFully(channel, all, 0);
        return RecordBatch.readAll(all.flip());
    }

    /**
     * Removes every batch whose last offset is {@code >= offset}.
     *
     * @return bytes removed
     */
    public int truncateTo(long offset) throws IOException {
        int pos = translateOffset(offset, size);
        if (pos < 0) {
            return 0;
        }
        int removed = size - pos;
        ByteBuffer header = ByteBuffer.allocate(8);
        FileSlice.readFully(channel, header, pos);
        long firstRemoved = header.getLong(0);
        offsetIndex.truncateTo(firstRemoved);
        timeIndex.truncateTo(firstRemoved);
        channel.truncate(pos);
        channel.position(pos);
        size = pos;
        if (pos == 0) {
            nextOffset = baseOffset;
            maxTimestamp = HybridTime.NO_TIMESTAMP;
            offsetIndex.reset();
            timeIndex.reset();
            bytesSinceLastIndexEntry = 0;
        } else {
            loadTail();
            maxTimestamp = lastBatchMaxTimestamp();
        }
        return removed;
    }

    public void flush() throws IOException {
        channel.force(false);
    }

    /** Called when the segment stops being the active one. */
    public void onBecomeInactive() throws IOException {
        offsetIndex.trimToValidSize();
        timeIndex.trimToValidSize();
        offsetIndex.flush();
        timeIndex.flush();
        channel.force(true);
    }

    void makeWritable() throws IOException {
        offsetIndex.makeWritable(config.maxIndexBytes());
        timeIndex.makeWritable(config.maxIndexBytes());
        channel.position(size);
    }

    /** Renames all three files, swapping {@code from} for {@code to} at the end of each name. */
    void changeSuffix(String from, String to) throws IOException {
        logFile = moveWithSuffix(logFile, from, to);
        offsetIndex.renameTo(swapSuffix(offsetIndex.file(), from, to));
        timeIndex.renameTo(swapSuffix(timeIndex.file(), from, to));
    }

    private static Path moveWithSuffix(Path file, String from, String to) throws IOException {
        Path target = swapSuffix(file, from, to);
        Files.move(file, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        return target;
    }

    private static Path swapSuffix(Path file, String from, String to) {
        String name = file.getFileName().toString();
        if (!name.endsWith(from)) {
            throw new IllegalStateException(file + " does not end with " + from);
        }
        return file.resolveSibling(name.substring(0, name.length() - from.length()) + to);
    }

    @Override
    public void close() throws IOException {
        if (!channel.isOpen()) {
            return;
        }
        offsetIndex.close();
        timeIndex.close();
        channel.close();
    }

    public void delete() throws IOException {
        close();
        Files.deleteIfExists(logFile);
        offsetIndex.deleteIfExists();
        timeIndex.deleteIfExists();
    }

    @Override
    public String toString() {
        return "LogSegment(base=" + baseOffset + ", next=" + nextOffset + ", size=" + size + ")";
    }
}
