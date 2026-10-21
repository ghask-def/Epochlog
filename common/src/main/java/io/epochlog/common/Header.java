package io.epochlog.common;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * A record header: a string key and an optional byte value.
 *
 * <p>Thread-safety: effectively immutable; callers must not mutate the value array.
 */
public final class Header {
    private final String key;
    private final byte[] value;

    public Header(String key, byte[] value) {
        this.key = Objects.requireNonNull(key, "key");
        this.value = value;
    }

    public static Header of(String key, String value) {
        return new Header(key, value == null ? null : value.getBytes(StandardCharsets.UTF_8));
    }

    public String key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Header h && key.equals(h.key) && Arrays.equals(value, h.value);
    }

    @Override
    public int hashCode() {
        return 31 * key.hashCode() + Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return key + "=" + (value == null ? "null" : new String(value, StandardCharsets.UTF_8));
    }
}
