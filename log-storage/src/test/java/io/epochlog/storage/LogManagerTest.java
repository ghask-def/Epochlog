package io.epochlog.storage;

import static io.epochlog.storage.TestBatches.keyed;
import static io.epochlog.storage.TestBatches.sized;
import static org.assertj.core.api.Assertions.assertThat;

import io.epochlog.common.Config;
import io.epochlog.common.ManualTime;
import io.epochlog.common.TopicPartition;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LogManagerTest {
    @TempDir
    Path dir;

    private final ManualTime time = new ManualTime();

    private LogManager manager(long cleanerIntervalMs) {
        return new LogManager(dir, LogConfig.from(Config.empty().with(LogConfig.SEGMENT_BYTES, 2048)), time,
                60_000, cleanerIntervalMs);
    }

    @Test
    void createsLogsAndReloadsThemWithTheirConfig() throws Exception {
        TopicPartition a = new TopicPartition("alpha", 0);
        TopicPartition b = new TopicPartition("beta-topic", 3);
        try (LogManager m = manager(60_000)) {
            m.startup();
            Log la = m.getOrCreate(a, m.defaults());
            assertThat(m.getOrCreate(a, m.defaults())).isSameAs(la);
            Log lb = m.getOrCreate(b, LogConfig.from(Config.empty().with(LogConfig.CLEANUP_POLICY, "compact")));
            la.appendAsLeader(sized(3, 10), 0, -1);
            lb.appendAsLeader(keyed("k", "v"), 0, -1);
            assertThat(m.all()).hasSize(2);
        }
        Files.createDirectories(dir.resolve("not-a-partition-dir-x"));
        try (LogManager m = manager(60_000)) {
            m.startup();
            assertThat(m.snapshot().keySet()).containsExactlyInAnyOrder(a, b);
            assertThat(m.get(a).orElseThrow().logEndOffset()).isEqualTo(3);
            assertThat(m.get(b).orElseThrow().config().compact()).isTrue();
            m.delete(a);
            assertThat(m.get(a)).isEmpty();
            assertThat(dir.resolve(a.toString())).doesNotExist();
        }
    }

    @Test
    void retentionRespectsUpperBound() throws Exception {
        TopicPartition tp = new TopicPartition("ret", 0);
        try (LogManager m = manager(60_000)) {
            m.startup();
            Log log = m.getOrCreate(tp, LogConfig.from(Config.empty()
                    .with(LogConfig.SEGMENT_BYTES, 2048).with(LogConfig.RETENTION_BYTES, 0)));
            for (int i = 0; i < 20; i++) {
                log.appendAsLeader(sized(1, 600), 0, -1);
            }
            m.setRetentionUpperBound(l -> 0);
            m.runRetention();
            assertThat(log.logStartOffset()).isZero();
            m.setRetentionUpperBound(Log::logEndOffset);
            m.runRetention();
            assertThat(log.logStartOffset()).isPositive();
        }
    }

    @Test
    void backgroundCleanerCompactsCompactedLogs() throws Exception {
        TopicPartition tp = new TopicPartition("cmp", 0);
        try (LogManager m = manager(20)) {
            m.startup();
            Log log = m.getOrCreate(tp, LogConfig.from(Config.empty()
                    .with(LogConfig.SEGMENT_BYTES, 2048).with(LogConfig.CLEANUP_POLICY, "compact")));
            for (int i = 0; i < 300; i++) {
                log.appendAsLeader(keyed("k" + (i % 5), "value-" + i), 0, -1);
            }
            long before = log.size();
            long deadline = System.currentTimeMillis() + 5_000;
            while (log.size() >= before / 2 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertThat(log.size()).isLessThan(before / 2);
        }
    }

    @Test
    void closeAbruptlyLeavesNoCleanShutdownMarker() throws Exception {
        TopicPartition tp = new TopicPartition("crash", 0);
        LogManager m = manager(60_000);
        m.startup();
        m.getOrCreate(tp, m.defaults()).appendAsLeader(sized(1, 10), 0, -1);
        m.closeAbruptly();
        assertThat(dir.resolve(tp.toString()).resolve(Log.CLEAN_SHUTDOWN_FILE)).doesNotExist();
        try (LogManager again = manager(60_000)) {
            again.startup();
            assertThat(again.get(tp).orElseThrow().logEndOffset()).isEqualTo(1);
        }
    }

    @Test
    void configRoundTrips() {
        LogConfig c = LogConfig.from(Config.empty().with(LogConfig.RETENTION_MS, "1h")
                .with(LogConfig.CLEANUP_POLICY, "compact").with(LogConfig.MAX_MESSAGE_BYTES, "2m"));
        assertThat(LogConfig.from(new Config(c.toMap()))).isEqualTo(c);
        assertThat(c.retentionMs()).isEqualTo(3_600_000);
        assertThat(c.maxBatchBytes()).isEqualTo(2 << 20);
    }
}
