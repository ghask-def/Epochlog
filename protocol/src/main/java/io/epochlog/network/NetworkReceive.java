package io.epochlog.network;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * Reads one size-prefixed frame.
 *
 * <p>Thread-safety: not thread-safe.
 */
final class NetworkReceive {
    private final ByteBuffer sizeBuf = ByteBuffer.allocate(4);
    private final int maxSize;
    private ByteBuffer payload;

    NetworkReceive(int maxSize) {
        this.maxSize = maxSize;
    }

    /** Returns true once the whole frame has arrived. */
    boolean readFrom(ReadableByteChannel ch) throws IOException {
        if (payload == null) {
            if (ch.read(sizeBuf) < 0) {
                throw new EOFException("connection closed by peer");
            }
            if (sizeBuf.hasRemaining()) {
                return false;
            }
            int size = sizeBuf.getInt(0);
            if (size < 0 || size > maxSize) {
                throw new IOException("invalid frame size " + size + " (max " + maxSize + ")");
            }
            payload = ByteBuffer.allocate(size);
        }
        if (payload.hasRemaining() && ch.read(payload) < 0) {
            throw new EOFException("connection closed by peer");
        }
        return !payload.hasRemaining();
    }

    ByteBuffer payload() {
        return payload.flip();
    }
}
