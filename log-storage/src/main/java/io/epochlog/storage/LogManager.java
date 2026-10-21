package io.epochlog.storage;

import io.epochlog.common.AtomicFiles;
import io.epochlog.common.Config;
import io.epochlog.common.Time;
import io.epochlog.common.TopicPartition;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns every partition log under a data directory and runs background maintenance: retention,
 * recovery point checkpoints and compaction.
 *
 * <p>Thread-safety: thread-safe.
 */
public final class LogManager implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(LogManager.class);
    static final String CONFIG_FILE = "partition.properties";

    private final Path dataDir;
    private final LogConfig defaults;
    private final Time time;
    private final long retentionCheckMs;
    private final ConcurrentHashMap<TopicPartition, Log> logs = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    private final LogCleaner cleaner;
    private volatile ToLongFunction<Log> retentionUpperBound = Log::logEndOffset;

    public LogManager(Path dataDir, LogConfig defaults, Time time, long retentionCheckMs, long cleanerIntervalMs) {
        this.dataDir = dataDir;
        this.defaults = defaults;
        this.time = time;
        this.retentionCheckMs = retentionCheckMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("log-maintenance").daemon().factory());
        this.cleaner = new LogCleaner(logs::values, cleanerIntervalMs);
    }

    public void startup() throws IOException {
        Files.createDirectories(dataDir);
        List<Path> dirs;
        try (Stream<Path> s = Files.list(dataDir)) {
            dirs = s.filter(Files::isDirectory).filter(p -> p.getFileName().toString().contains("-")).toList();
        }
        for (Path dir : dirs) {
            TopicPartition tp;
            try {
                tp = TopicPartition.parse(dir.getFileName().toString());
            } catch (IllegalArgumentException e) {
                continue;
            }
            LogConfig cfg = readConfig(dir);
            logs.put(tp, Log.open(dir, tp, cfg, time));
        }
        LOG.info("loaded {} logs from {}", logs.size(), dataDir);
        scheduler.scheduleWithFixedDelay(this::runRetention, retentionCheckMs, retentionCheckMs, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::checkpointRecoveryPoints, 10, 10, TimeUnit.SECONDS);
        cleaner.start();
    }

    private LogConfig readConfig(Path dir) throws IOException {
        Path p = dir.resolve(CONFIG_FILE);
        if (!Files.exists(p)) {
            return defaults;
        }
        return LogConfig.from(new Config(defaults.toMap()).withAll(Config.load(p).asMap()));
    }

    /** Returns the existing log or creates one with {@code config}. */
    public Log getOrCreate(TopicPartition tp, LogConfig config) throws IOException {
        Log existing = logs.get(tp);
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            existing = logs.get(tp);
            if (existing != null) {
                return existing;
            }
            Path dir = dataDir.resolve(tp.toString());
            Files.createDirectories(dir);
            String props = config.toMap().entrySet().stream()
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining("\n", "", "\n"));
            AtomicFiles.write(dir.resolve(CONFIG_FILE), props);
            Log log = Log.open(dir, tp, config, time);
            logs.put(tp, log);
            return log;
        }
    }

    public Optional<Log> get(TopicPartition tp) {
        return Optional.ofNullable(logs.get(tp));
    }

    public Collection<Log> all() {
        return logs.values();
    }

    public Map<TopicPartition, Log> snapshot() {
        return Map.copyOf(logs);
    }

    public LogConfig defaults() {
        return defaults;
    }

    /** Sets the offset retention may not delete past, normally the high watermark. */
    public void setRetentionUpperBound(ToLongFunction<Log> bound) {
        this.retentionUpperBound = bound;
    }

    public void runRetention() {
        for (Log log : logs.values()) {
            try {
                log.deleteOldSegments(retentionUpperBound.applyAsLong(log));
            } catch (IOException | RuntimeException e) {
                LOG.warn("retention failed for {}", log.topicPartition(), e);
            }
        }
    }

    private void checkpointRecoveryPoints() {
        for (Log log : logs.values()) {
            try {
                log.checkpointRecoveryPoint();
            } catch (IOException | RuntimeException e) {
                LOG.warn("recovery point checkpoint failed for {}", log.topicPartition(), e);
            }
        }
    }

    public void delete(TopicPartition tp) throws IOException {
        Log log = logs.remove(tp);
        if (log != null) {
            log.delete();
        }
    }

    private void stopBackground() {
        scheduler.shutdownNow();
        cleaner.close();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() throws IOException {
        stopBackground();
        IOException first = null;
        for (Log log : logs.values()) {
            try {
                log.close();
            } catch (IOException e) {
                if (first == null) {
                    first = e;
                }
            }
        }
        if (first != null) {
            throw first;
        }
    }

    /** Stops without flushing or writing clean-shutdown markers. Used to simulate a crash. */
    public void closeAbruptly() throws IOException {
        stopBackground();
        for (Log log : logs.values()) {
            log.closeAbruptly();
        }
    }
}
