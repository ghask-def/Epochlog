package io.epochlog.common;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * CRC32C helpers. Backed by {@link CRC32C}, which the JIT turns into hardware instructions on
 * x86 and aarch64.
 *
 * <p>Thread-safety: thread-safe (stateless).
 */
public final class Crc32c {
    private Crc32c() {}

    /** Computes the checksum of {@code length} bytes starting at {@code offset}, without moving the buffer. */
    public static int compute(ByteBuffer buffer, int offset, int length) {
        CRC32C crc = new CRC32C();
        ByteBuffer view = buffer.duplicate();
        view.limit(offset + length).position(offset);
        crc.update(view);
        return (int) crc.getValue();
    }

    public static int compute(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, 0, bytes.length);
        return (int) crc.getValue();
    }
}
