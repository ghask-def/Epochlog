package io.epochlog.controller;

import io.epochlog.common.Config;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Command-line entry point: {@code epochlog-controller [config.properties] [key=value ...]}.
 * {@code EPOCHLOG_SOME_KEY} environment variables set {@code some.key}.
 *
 * <p>Thread-safety: not applicable (entry point).
 */
public final class ControllerMain {
    private ControllerMain() {}

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
        Controller controller = new Controller(ControllerConfig.from(config.withAll(overrides)));
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            controller.close();
            stopped.countDown();
        }, "shutdown"));
        controller.start();
        stopped.await();
    }
}
