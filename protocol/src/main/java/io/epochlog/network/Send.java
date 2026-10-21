package io.epochlog.network;

import io.epochlog.protocol.Records;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.List;

/**
 * An outgoing frame made of heap buffers and {@link Records.Transferable} regions. Consecutive
 * buffers go out with one gathering write; transferable regions use {@code transferTo} so file
 * data reaches the socket without passing through the heap.
 *
 * <p>Thread-safety: not thread-safe. Owned by one network thread once queued.
 */
public final class Send {
    private final List<Object> parts;
    private final long size;
    private int index;
    private long partWritten;
    private long written;

    public Send(List<Object> parts, long size) {
        for (Object p : parts) {
            if (!(p instanceof ByteBuffer) && !(p instanceof Records.Transferable)) {
                throw new IllegalArgumentException("unsupported part " + p.getClass());
            }
        }
        this.parts = parts;
        this.size = size;
    }

    public long size() {
        return size;
    }

    public boolean completed() {
        return written >= size;
    }

    /** Writes as much as the channel accepts. Returns bytes written by this call. */
    public long writeTo(GatheringByteChannel ch) throws IOException {
        long total = 0;
        while (index < parts.size()) {
            Object part = parts.get(index);
            if (part instanceof ByteBuffer) {
                int end = index;
                while (end < parts.size() && parts.get(end) instanceof ByteBuffer) {
                    end++;
                }
                ByteBuffer[] run = new ByteBuffer[end - index];
                for (int i = 0; i < run.length; i++) {
                    run[i] = (ByteBuffer) parts.get(index + i);
                }
                long n = ch.write(run);
                total += n;
                written += n;
                while (index < end && !((ByteBuffer) parts.get(index)).hasRemaining()) {
                    index++;
                }
                if (index < end) {
                    return total;
                }
            } else {
                Records.Transferable t = (Records.Transferable) part;
                long remaining = t.sizeInBytes() - partWritten;
                long n = remaining == 0 ? 0 : t.transferTo(ch, partWritten, remaining);
                partWritten += n;
                total += n;
                written += n;
                if (partWritten < t.sizeInBytes()) {
                    return total;
                }
                index++;
                partWritten = 0;
            }
        }
        return total;
    }
}
