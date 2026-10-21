package io.epochlog.storage;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Fixed-width entries in a memory-mapped file, searched with binary search. Active segments
 * preallocate the file to the maximum size; it is trimmed to the valid entries when the segment
 * rolls or closes, so on reopen {@code fileLength / entrySize} is the entry count.
 *
 * <p>Thread-safety: single-writer. One thread appends or truncates; any number of threads may
 * look up concurrently. An append stores the entry bytes and then writes the volatile entry
 * count, so a reader that sees count {@code n} also sees entries {@code [0, n)}.
 */
abstract class AbstractIndex implements Closeable {
    protected final long baseOffset;
    private final int entrySize;
    private volatile Path file;
    private volatile MappedByteBuffer mmap;
    private volatile int entries;
    private int maxEntries;

    AbstractIndex(Path file, long baseOffset, int maxIndexBytes, boolean writable, int entrySize) throws IOException {
        this.file = file;
        this.baseOffset = baseOffset;
        this.entrySize = entrySize;
        boolean existed = Files.exists(file);
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            long len = raf.length();
            int validEntries = existed ? (int) (len / entrySize) : 0;
            if (writable) {
                raf.setLength(roundDown(maxIndexBytes));
            } else if (len % entrySize != 0) {
                raf.setLength(roundDown((int) len));
            }
            this.mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, raf.length());
            this.maxEntries = (int) (raf.length() / entrySize);
            this.entries = Math.min(validEntries, maxEntries);
        }
    }

    private int roundDown(int bytes) {
        return bytes - bytes % entrySize;
    }

    public Path file() {
        return file;
    }

    public int entries() {
        return entries;
    }

    public boolean isFull() {
        return entries >= maxEntries;
    }

    public long sizeInBytes() {
        return (long) entries * entrySize;
    }

    protected MappedByteBuffer buffer() {
        return mmap;
    }

    protected int entrySize() {
        return entrySize;
    }

    /** Publishes a new entry count. Entry bytes must already be written. */
    protected void setEntries(int n) {
        entries = n;
    }

    protected int relativeOffset(long offset) {
        long rel = offset - baseOffset;
        if (rel < 0 || rel > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("offset " + offset + " not representable relative to " + baseOffset);
        }
        return (int) rel;
    }

    /**
     * Largest slot whose key is {@code <= target}, or -1 if every key is larger.
     * {@code n} is the entry count snapshot the caller read.
     */
    protected int largestLowerBound(long target, int n, MappedByteBuffer buf) {
        if (n == 0 || keyAt(buf, 0) > target) {
            return -1;
        }
        int lo = 0;
        int hi = n - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (keyAt(buf, mid) <= target) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return lo;
    }

    protected abstract long keyAt(MappedByteBuffer buf, int slot);

    /** Drops entries whose offset is {@code >= offset}. */
    public void truncateTo(long offset) {
        int n = entries;
        MappedByteBuffer buf = mmap;
        int keep = n;
        while (keep > 0 && offsetAt(buf, keep - 1) >= offset) {
            keep--;
        }
        entries = keep;
    }

    protected abstract long offsetAt(MappedByteBuffer buf, int slot);

    public void reset() {
        entries = 0;
    }

    /** Shrinks the file to the valid entries. Called when the segment stops being active. */
    public void trimToValidSize() throws IOException {
        resize((int) sizeInBytes());
    }

    /** Re-grows the file so it can take appends again, used when truncation reactivates a segment. */
    public void makeWritable(int maxIndexBytes) throws IOException {
        resize(Math.max(roundDown(maxIndexBytes), (int) sizeInBytes()));
    }

    private void resize(int newLength) throws IOException {
        int n = entries;
        mmap.force();
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.setLength(newLength);
            mmap = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, newLength);
            maxEntries = newLength / entrySize;
            entries = Math.min(n, maxEntries);
        }
    }

    public void flush() {
        mmap.force();
    }

    public void renameTo(Path target) throws IOException {
        Files.move(file, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        file = target;
    }

    @Override
    public void close() throws IOException {
        trimToValidSize();
    }

    public void deleteIfExists() throws IOException {
        Files.deleteIfExists(file);
    }
}
