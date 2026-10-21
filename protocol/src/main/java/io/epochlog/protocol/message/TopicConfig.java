package io.epochlog.protocol.message;

import io.epochlog.protocol.ProtocolReader;
import io.epochlog.protocol.ProtocolWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Topic-level config overrides.
 *
 * <p>Thread-safety: immutable.
 */
public record TopicConfig(String topic, Map<String, String> configs) {

    public TopicConfig {
        configs = Map.copyOf(configs);
    }

    public void write(ProtocolWriter w) {
        w.putString(topic);
        writeMap(w, configs);
    }

    public static TopicConfig read(ProtocolReader r) {
        return new TopicConfig(r.getString(), readMap(r));
    }

    static void writeMap(ProtocolWriter w, Map<String, String> m) {
        w.putArray(m.entrySet(), (out, e) -> out.putString(e.getKey()).putString(e.getValue()));
    }

    static Map<String, String> readMap(ProtocolReader r) {
        List<String[]> pairs = r.getArray(in -> new String[] {in.getString(), in.getString()});
        Map<String, String> m = new LinkedHashMap<>();
        for (String[] p : pairs) {
            m.put(p[0], p[1]);
        }
        return m;
    }
}
