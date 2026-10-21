package io.epochlog.protocol;

import io.epochlog.network.Send;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Serializes messages. Fixed-size fields go into a growing heap buffer; {@link Records.Transferable}
 * payloads are kept as separate parts so the network layer can send them with
 * {@code transferTo} instead of copying.
 *
 * <p>Thread-safety: not thread-safe.
 */
public final class ProtocolWriter {
    private final List<Object> parts = new ArrayList<>();
    private ByteBuffer current = ByteBuffer.allocate(256);
    private long size;

    private void ensure(int n) {
        if (current.remaining() >= n) {
            return;
        }
        ByteBuffer bigger = ByteBuffer.allocate(Math.max(current.capacity() * 2, current.position() + n));
        current.flip();
        bigger.put(current);
        current = bigger;
    }

    public ProtocolWriter putByte(byte v) {
        ensure(1);
        current.put(v);
        size += 1;
        return this;
    }

    public ProtocolWriter putBoolean(boolean v) {
        return putByte(v ? (byte) 1 : (byte) 0);
    }

    public ProtocolWriter putShort(short v) {
        ensure(2);
        current.putShort(v);
        size += 2;
        return this;
    }

    public ProtocolWriter putInt(int v) {
        ensure(4);
        current.putInt(v);
        size += 4;
        return this;
    }

    public ProtocolWriter putLong(long v) {
        ensure(8);
        current.putLong(v);
        size += 8;
        return this;
    }

    public ProtocolWriter putString(String s) {
        if (s == null) {
            return putShort((short) -1);
        }
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        if (b.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("string too long");
        }
        putShort((short) b.length);
        ensure(b.length);
        current.put(b);
        size += b.length;
        return this;
    }

    public ProtocolWriter putBytes(byte[] b) {
        if (b == null) {
            return putInt(-1);
        }
        putInt(b.length);
        ensure(b.length);
        current.put(b);
        size += b.length;
        return this;
    }

    public ProtocolWriter putIntList(List<Integer> values) {
        putInt(values.size());
        for (int v : values) {
            putInt(v);
        }
        return this;
    }

    public <T> ProtocolWriter putArray(Collection<T> items, BiConsumer<ProtocolWriter, T> writeOne) {
        putInt(items.size());
        for (T item : items) {
            writeOne.accept(this, item);
        }
        return this;
    }

    public ProtocolWriter putRecords(Records records) {
        if (records == null) {
            return putInt(-1);
        }
        putInt(records.sizeInBytes());
        if (records.sizeInBytes() == 0) {
            return this;
        }
        flushCurrent();
        if (records instanceof MemoryRecords m) {
            parts.add(m.buffer().duplicate());
        } else {
            parts.add(records);
        }
        size += records.sizeInBytes();
        return this;
    }

    private void flushCurrent() {
        if (current.position() > 0) {
            current.flip();
            parts.add(current);
            current = ByteBuffer.allocate(256);
        }
    }

    public long size() {
        return size;
    }

    /** A size-prefixed frame ready for the socket. */
    public Send toSend() {
        flushCurrent();
        List<Object> all = new ArrayList<>(parts.size() + 1);
        all.add(ByteBuffer.allocate(4).putInt(0, Math.toIntExact(size)));
        all.addAll(parts);
        return new Send(all, size + 4);
    }

    /** The message bytes without a size prefix. Only valid when nothing is transferable. */
    public ByteBuffer toByteBuffer() {
        flushCurrent();
        ByteBuffer out = ByteBuffer.allocate(Math.toIntExact(size));
        for (Object p : parts) {
            if (p instanceof ByteBuffer b) {
                out.put(b.duplicate());
            } else {
                throw new IllegalStateException("cannot flatten transferable records");
            }
        }
        return out.flip();
    }
}
