package io.epochlog.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Compression codecs for the records section of a batch. Stored in the low three bits of the
 * batch attributes.
 *
 * <p>Thread-safety: thread-safe (stateless).
 */
public enum CompressionType {
    NONE(0),
    GZIP(1);

    public final int id;

    CompressionType(int id) {
        this.id = id;
    }

    public static CompressionType forId(int id) {
        return switch (id) {
            case 0 -> NONE;
            case 1 -> GZIP;
            default -> throw new CorruptRecordException("unknown compression type " + id);
        };
    }

    public static CompressionType forName(String name) {
        return valueOf(name.trim().toUpperCase());
    }

    byte[] compress(byte[] src, int len) {
        if (this == NONE) {
            throw new IllegalStateException();
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream(Math.max(64, len / 2));
        try (GZIPOutputStream gz = new GZIPOutputStream(bos, 8192)) {
            gz.write(src, 0, len);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bos.toByteArray();
    }

    byte[] decompress(byte[] src) {
        if (this == NONE) {
            return src;
        }
        try (GZIPInputStream gz = new GZIPInputStream(new ByteArrayInputStream(src), 8192)) {
            return gz.readAllBytes();
        } catch (IOException e) {
            throw new CorruptRecordException("failed to decompress batch", e);
        }
    }
}
