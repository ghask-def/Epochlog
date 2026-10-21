package io.epochlog.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Deserializes messages from a received frame.
 *
 * <p>Thread-safety: not thread-safe.
 */
public final class ProtocolReader {
    private static final int MAX_ARRAY = 1 << 24;

    private final ByteBuffer buf;

    public ProtocolReader(ByteBuffer buf) {
        this.buf = buf;
    }

    public byte getByte() {
        return buf.get();
    }

    public boolean getBoolean() {
        return buf.get() != 0;
    }

    public short getShort() {
        return buf.getShort();
    }

    public int getInt() {
        return buf.getInt();
    }

    public long getLong() {
        return buf.getLong();
    }

    public String getString() {
        short len = buf.getShort();
        if (len < 0) {
            return null;
        }
        String s = new String(buf.array(), buf.arrayOffset() + buf.position(), len, StandardCharsets.UTF_8);
        buf.position(buf.position() + len);
        return s;
    }

    public byte[] getBytes() {
        int len = buf.getInt();
        if (len < 0) {
            return null;
        }
        byte[] b = new byte[len];
        buf.get(b);
        return b;
    }

    public List<Integer> getIntList() {
        int n = checkedCount();
        List<Integer> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(buf.getInt());
        }
        return out;
    }

    public <T> List<T> getArray(Function<ProtocolReader, T> readOne) {
        int n = checkedCount();
        List<T> out = new ArrayList<>(Math.min(n, 1024));
        for (int i = 0; i < n; i++) {
            out.add(readOne.apply(this));
        }
        return out;
    }

    /** Records as a zero-copy slice of the frame. */
    public MemoryRecords getRecords() {
        int len = buf.getInt();
        if (len < 0) {
            return null;
        }
        ByteBuffer slice = buf.slice(buf.position(), len);
        buf.position(buf.position() + len);
        return new MemoryRecords(slice);
    }

    private int checkedCount() {
        int n = buf.getInt();
        if (n < 0 || n > MAX_ARRAY || n > buf.remaining()) {
            throw new IllegalArgumentException("bad array length " + n);
        }
        return n;
    }

    public int remaining() {
        return buf.remaining();
    }
}
