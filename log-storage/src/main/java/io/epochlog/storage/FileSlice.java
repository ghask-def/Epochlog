package io.epochlog.storage;

import io.epochlog.common.RecordBatch;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 * A byte range of a segment file holding whole batches. Fetch responses send these with
 * {@link FileChannel#transferTo} so record bytes go from page cache to socket without a copy
 * through the heap.
 *
 * @param firstBatchOffset base offset of the first batch in the slice
 *
 * <p>Thread-safety: immutable; reads use positional I/O and can run concurrently.
 */
public record FileSlice(FileChannel channel, long position, int size, long firstBatchOffset) {

    public ByteBuffer readFully() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(size);
        readFully(channel, buf, position);
        return buf.flip();
    }

    public List<RecordBatch> batches() throws IOException {
        return RecordBatch.readAll(readFully());
    }

    /** Transfers up to {@code count} bytes starting {@code offset} bytes into the slice. */
    public long transferTo(WritableByteChannel target, long offset, long count) throws IOException {
        long n = Math.min(count, size - offset);
        return channel.transferTo(position + offset, n, target);
    }

    static void readFully(FileChannel ch, ByteBuffer dst, long position) throws IOException {
        long pos = position;
        while (dst.hasRemaining()) {
            int n = ch.read(dst, pos);
            if (n < 0) {
                throw new EOFException("unexpected end of file at " + pos);
            }
            pos += n;
        }
    }
}
