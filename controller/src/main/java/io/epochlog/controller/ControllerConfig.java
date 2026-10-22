package io.epochlog.controller;

import io.epochlog.common.Config;
import java.nio.file.Path;

/**
 * Controller settings.
 *
 * <p>Thread-safety: immutable.
 */
public record ControllerConfig(String host, int port, Path dataDir, long sessionTimeoutMs, long checkIntervalMs,
        long requestTimeoutMs, int defaultReplicationFactor) {

    public static ControllerConfig from(Config c) {
        return new ControllerConfig(
                c.getString("host", "0.0.0.0"),
                c.getInt("port", 9090),
                Path.of(c.getString("data.dir", "data/controller")),
                c.getDurationMs("broker.session.timeout.ms", 6_000),
                c.getDurationMs("failure.check.interval.ms", 100),
                c.getDurationMs("request.timeout.ms", 10_000),
                c.getInt("default.replication.factor", 3));
    }
}
