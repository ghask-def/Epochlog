package io.epochlog.broker;

import io.epochlog.common.Config;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Command-line entry point: {@code epochlog-broker [config.properties] [key=value ...]}.
 * Environment variables named {@code EPOCHLOG_SOME_KEY} also set {@code some.key}, which is
 * handy in containers.
 *
 * <p>Thread-safety: not applicable (entry point).
 */
public final class BrokerMain {
    private BrokerMain() {}

    public static void main(String[] args) throws Exception {
        Config config = Config.empty();
        Map<String, String> overrides = new HashMap<>();
        System.getenv().forEach((k, v) -> {
            if (k.startsWith("EPOCHLOG_")) {
                overrides.put(k.substring(9).toLowerCase().replace('_', '.'), v);
            }
        });
        for (String arg : args) {
            int eq = arg.indexOf('=');
            if (eq > 0) {
                overrides.put(arg.substring(0, eq), arg.substring(eq + 1));
            } else {
                config = Config.load(Path.of(arg));
            }
        }
        Broker broker = new Broker(BrokerConfig.from(config.withAll(overrides)));
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            broker.close();
            stopped.countDown();
        }, "shutdown"));
        broker.start();
        stopped.await();
    }
}
