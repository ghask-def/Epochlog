package io.epochlog.common;

import java.nio.ByteBuffer;

/**
 * Zig-zag varint encoding, the same scheme protobuf uses for sint32/sint64.
 *
 * <p>Thread-safety: thread-safe (stateless).
 */
public final class ByteUtils {
    private ByteUtils() {}

    public static void writeVarint(int value, ByteBuffer out) {
        writeUnsignedVarint((value << 1) ^ (value >> 31), out);
    }

    public static void writeVarlong(long value, ByteBuffer out) {
        long v = (value << 1) ^ (value >> 63);
        while ((v & 0xffffffffffffff80L) != 0L) {
            out.put((byte) ((v & 0x7f) | 0x80));
            v >>>= 7;
        }
        out.put((byte) v);
    }

    public static void writeUnsignedVarint(int value, ByteBuffer out) {
        while ((value & 0xffffff80) != 0) {
            out.put((byte) ((value & 0x7f) | 0x80));
            value >>>= 7;
        }
        out.put((byte) value);
    }

    public static int readVarint(ByteBuffer in) {
        int raw = readUnsignedVarint(in);
        return (raw >>> 1) ^ -(raw & 1);
    }

    public static int readUnsignedVarint(ByteBuffer in) {
        int value = 0;
        int shift = 0;
        while (true) {
            byte b = in.get();
            value |= (b & 0x7f) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
            shift += 7;
            if (shift > 28) {
                throw new CorruptRecordException("varint too long");
            }
        }
    }

    public static long readVarlong(ByteBuffer in) {
        long raw = 0;
        int shift = 0;
        while (true) {
            byte b = in.get();
            raw |= (long) (b & 0x7f) << shift;
            if ((b & 0x80) == 0) {
                return (raw >>> 1) ^ -(raw & 1);
            }
            shift += 7;
            if (shift > 63) {
                throw new CorruptRecordException("varlong too long");
            }
        }
    }

    public static int sizeOfVarint(int value) {
        return sizeOfUnsignedVarint((value << 1) ^ (value >> 31));
    }

    public static int sizeOfUnsignedVarint(int value) {
        int bytes = 1;
        while ((value & 0xffffff80) != 0) {
            bytes++;
            value >>>= 7;
        }
        return bytes;
    }

    public static int sizeOfVarlong(long value) {
        long v = (value << 1) ^ (value >> 63);
        int bytes = 1;
        while ((v & 0xffffffffffffff80L) != 0L) {
            bytes++;
            v >>>= 7;
        }
        return bytes;
    }
}
