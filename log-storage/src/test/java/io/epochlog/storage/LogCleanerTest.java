package io.epochlog.storage;

import static io.epochlog.storage.TestBatches.keyed;
import static org.assertj.core.api.Assertions.assertThat;

import io.epochlog.common.Config;
import io.epochlog.common.ManualTime;
import io.epochlog.common.Record;
import io.epochlog.common.RecordBatch;
import io.epochlog.common.TopicPartition;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LogCleanerTest {
    private static final TopicPartition TP = new TopicPartition("compacted", 0);

    @TempDir
    Path dir;

    private final ManualTime time = new ManualTime();

    private LogConfig config() {
        return LogConfig.from(Config.empty()
                .with(LogConfig.SEGMENT_BYTES, 2048)
                .with(LogConfig.CLEANUP_POLICY, "compact"));
    }

    private static List<Record> all(Log log) throws Exception {
        List<Record> out = new ArrayList<>();
        for (LogSegment seg : log.segments()) {
            for (RecordBatch b : seg.readAllBatches()) {
                out.addAll(b.records());
            }
        }
        return out;
    }

    @Test
    void keepsLatestValuePerKeyAndPreservesOffsets() throws Exception {
        Map<String, Long> expected = new HashMap<>();
        try (Log log = Log.open(dir, TP, config(), time)) {
            for (int i = 0; i < 300; i++) {
                String key = "k" + (i % 10);
                LogAppendInfo info = log.appendAsLeader(keyed(key, "v" + i), 0, -1);
                expected.put(key, info.firstOffset());
            }
            long endBefore = log.logEndOffset();
            long removed = LogCleaner.clean(log);
            assertThat(removed).isPositive();
            assertThat(log.logEndOffset()).isEqualTo(endBefore);

            List<Record> records = all(log);
            Map<String, Long> latest = new HashMap<>();
            long prev = -1;
            for (Record r : records) {
                assertThat(r.offset()).isGreaterThan(prev);
                prev = r.offset();
                latest.put(new String(r.key(), StandardCharsets.UTF_8), r.offset());
            }
            assertThat(latest).isEqualTo(expected);
            assertThat(log.read(0, 1 << 20, Long.MAX_VALUE)).isNotNull();
        }

        try (Log reopened = Log.open(dir, TP, config(), time)) {
            assertThat(reopened.logEndOffset()).isEqualTo(300);
            assertThat(all(reopened)).hasSizeLessThan(300);
        }
    }

    @Test
    void finishesInterruptedSwapOnLoad() throws Exception {
        long secondBase;
        try (Log log = Log.open(dir, TP, config(), time)) {
            for (int i = 0; i < 100; i++) {
                log.appendAsLeader(keyed("same", "v" + i), 0, -1);
            }
            secondBase = log.closedSegments().get(1).baseOffset();
        }
        // Pretend the cleaner crashed right after renaming its output to .swap: copy the
        // segment's own bytes into a swap file so we can tell the result is still readable.
        String prefix = LogSegment.filenamePrefix(secondBase);
        Files.copy(dir.resolve(prefix + ".log"), dir.resolve(prefix + ".log.swap"));
        Files.writeString(dir.resolve(prefix + ".index.cleaned"), "junk");

        try (Log log = Log.open(dir, TP, config(), time)) {
            assertThat(log.logEndOffset()).isEqualTo(100);
            assertThat(all(log)).hasSize(100);
        }
        try (Stream<Path> files = Files.list(dir)) {
            assertThat(files.map(p -> p.getFileName().toString()))
                    .noneMatch(n -> n.endsWith(".swap") || n.endsWith(".cleaned"));
        }
    }
}
