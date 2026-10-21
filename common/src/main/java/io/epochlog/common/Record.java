package io.epochlog.common;

import java.util.Arrays;
import java.util.List;

/**
 * A single decoded record with absolute offset and timestamp.
 *
 * <p>Thread-safety: effectively immutable; callers must not mutate the returned arrays.
 */
public final class Record {
    private final long offset;
    private final long timestamp;
    private final byte[] key;
    private final byte[] value;
    private final List<Header> headers;

    public Record(long offset, long timestamp, byte[] key, byte[] value, List<Header> headers) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.headers = List.copyOf(headers);
    }

    public long offset() {
        return offset;
    }

    /** Hybrid logical timestamp, see {@link HybridTime}. */
    public long timestamp() {
        return timestamp;
    }

    public byte[] key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    public List<Header> headers() {
        return headers;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Record r
                && offset == r.offset
                && timestamp == r.timestamp
                && Arrays.equals(key, r.key)
                && Arrays.equals(value, r.value)
                && headers.equals(r.headers);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(offset) * 31 + Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "Record(offset=" + offset + ", ts=" + HybridTime.format(timestamp)
                + ", keySize=" + (key == null ? -1 : key.length)
                + ", valueSize=" + (value == null ? -1 : value.length) + ")";
    }
}
