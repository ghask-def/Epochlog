package io.epochlog.common;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * String key/value configuration with typed getters. Durations accept a suffix
 * ({@code ms}, {@code s}, {@code m}, {@code h}); sizes accept {@code k}, {@code m}, {@code g}.
 *
 * <p>Thread-safety: immutable.
 */
public final class Config {
    private final Map<String, String> values;

    public Config(Map<String, String> values) {
        this.values = Collections.unmodifiableMap(new HashMap<>(values));
    }

    public static Config empty() {
        return new Config(Map.of());
    }

    public static Config load(Path file) throws IOException {
        Properties props = new Properties();
        try (Reader r = Files.newBufferedReader(file)) {
            props.load(r);
        }
        Map<String, String> m = new HashMap<>();
        props.forEach((k, v) -> m.put(k.toString().trim(), v.toString().trim()));
        return new Config(m);
    }

    public Config with(String key, Object value) {
        Map<String, String> m = new HashMap<>(values);
        m.put(key, String.valueOf(value));
        return new Config(m);
    }

    public Config withAll(Map<String, String> overrides) {
        Map<String, String> m = new HashMap<>(values);
        m.putAll(overrides);
        return new Config(m);
    }

    public boolean contains(String key) {
        return values.containsKey(key);
    }

    public String getString(String key, String def) {
        return values.getOrDefault(key, def);
    }

    public String getRequired(String key) {
        String v = values.get(key);
        if (v == null) {
            throw new EpochlogException("missing required config: " + key);
        }
        return v;
    }

    public int getInt(String key, int def) {
        String v = values.get(key);
        return v == null ? def : Math.toIntExact(parseSize(key, v));
    }

    public long getLong(String key, long def) {
        String v = values.get(key);
        return v == null ? def : parseSize(key, v);
    }

    public boolean getBoolean(String key, boolean def) {
        String v = values.get(key);
        return v == null ? def : Boolean.parseBoolean(v);
    }

    public long getDurationMs(String key, long def) {
        String v = values.get(key);
        if (v == null) {
            return def;
        }
        String s = v.toLowerCase();
        try {
            if (s.endsWith("ms")) return Long.parseLong(s.substring(0, s.length() - 2));
            if (s.endsWith("s")) return Long.parseLong(s.substring(0, s.length() - 1)) * 1000L;
            if (s.endsWith("m")) return Long.parseLong(s.substring(0, s.length() - 1)) * 60_000L;
            if (s.endsWith("h")) return Long.parseLong(s.substring(0, s.length() - 1)) * 3_600_000L;
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            throw new EpochlogException("bad duration for " + key + ": " + v);
        }
    }

    public Map<String, String> asMap() {
        return values;
    }

    private static long parseSize(String key, String v) {
        String s = v.toLowerCase();
        long mult = 1;
        if (s.endsWith("k")) mult = 1L << 10;
        else if (s.endsWith("m")) mult = 1L << 20;
        else if (s.endsWith("g")) mult = 1L << 30;
        if (mult != 1) s = s.substring(0, s.length() - 1);
        try {
            return Long.parseLong(s) * mult;
        } catch (NumberFormatException e) {
            throw new EpochlogException("bad number for " + key + ": " + v);
        }
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
