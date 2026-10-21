package io.epochlog.storage;

import static io.epochlog.storage.TestBatches.batch;
import static io.epochlog.storage.TestBatches.idempotent;
import static io.epochlog.storage.TestBatches.sized;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.epochlog.common.Config;
import io.epochlog.common.HybridTime;
import io.epochlog.common.ManualTime;
import io.epochlog.common.Record;
import io.epochlog.common.RecordBatch;
import io.epochlog.common.TopicPartition;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LogTest {
    private static final TopicPartition TP = new TopicPartition("events", 0);

    @TempDir
    Path dir;

    private final ManualTime time = new ManualTime();
    private Log log;

    private LogConfig config(String... kv) {
        Config c = Config.empty()
                .with(LogConfig.SEGMENT_BYTES, 4096)
                .with(LogConfig.INDEX_INTERVAL_BYTES, 256);
        for (int i = 0; i < kv.length; i += 2) {
            c = c.with(kv[i], kv[i + 1]);
        }
        return LogConfig.from(c);
    }

    private Log open(LogConfig cfg) throws Exception {
        log = Log.open(dir, TP, cfg, time);
        return log;
    }

    @AfterEach
    void tearDown() throws Exception {
        if (log != null) {
            log.close();
        }
    }

    private static List<Record> readAll(Log log, long from) throws Exception {
        List<Record> out = new ArrayList<>();
        long next = from;
        while (next < log.logEndOffset()) {
            FileSlice slice = log.read(next, 1024, Long.MAX_VALUE);
            for (RecordBatch b : slice.batches()) {
                for (Record r : b.records()) {
                    if (r.offset() >= next) {
                        out.add(r);
                    }
                }
                next = b.nextOffset();
            }
        }
        return out;
    }

    private static String str(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    @Test
    void appendAndReadBackAcrossSegments() throws Exception {
        open(config());
        for (int i = 0; i < 200; i++) {
            LogAppendInfo info = log.appendAsLeader(batch("a" + i, "b" + i), 0, -1);
            assertThat(info.firstOffset()).isEqualTo(2L * i);
        }
        assertThat(log.logEndOffset()).isEqualTo(400);
        assertThat(log.numSegments()).isGreaterThan(3);

        List<Record> records = readAll(log, 0);
        assertThat(records).hasSize(400);
        for (int i = 0; i < 400; i++) {
            assertThat(records.get(i).offset()).isEqualTo(i);
        }
        assertThat(str(records.get(399).value())).isEqualTo("b199");

        // Start mid-batch and mid-segment; the index must point at the batch holding the offset.
        List<Record> tail = readAll(log, 251);
        assertThat(tail.get(0).offset()).isEqualTo(251);
        assertThat(str(tail.get(0).value())).isEqualTo("b125");
    }

    @Test
    void readRespectsMaxOffsetAndAlwaysReturnsOneBatch() throws Exception {
        open(config());
        log.appendAsLeader(sized(1, 600), 0, -1);
        log.appendAsLeader(sized(1, 600), 0, -1);
        log.appendAsLeader(sized(1, 600), 0, -1);

        FileSlice upToOne = log.read(0, 1 << 20, 1);
        assertThat(upToOne.batches()).hasSize(1);

        FileSlice tiny = log.read(0, 10, Long.MAX_VALUE);
        assertThat(tiny.batches()).hasSize(1);

        assertThat(log.read(3, 1024, Long.MAX_VALUE)).isNull();
        assertThatThrownBy(() -> log.read(4, 1024, Long.MAX_VALUE)).isInstanceOf(OffsetOutOfRangeException.class);
    }

    @Test
    void cleanReopenKeepsEverything() throws Exception {
        open(config());
        for (int i = 0; i < 100; i++) {
            log.appendAsLeader(batch("v" + i), 3, HybridTime.pack(1000 + i, 0));
        }
        log.close();
        log = null;

        open(config());
        assertThat(log.logEndOffset()).isEqualTo(100);
        assertThat(log.latestEpoch()).isEqualTo(3);
        assertThat(readAll(log, 0)).hasSize(100);
        assertThat(log.lastTimestamp()).isEqualTo(HybridTime.pack(1099, 0));
        log.appendAsLeader(batch("after"), 3, -1);
        assertThat(log.logEndOffset()).isEqualTo(101);
    }

    @Test
    void recoveryTruncatesTornWrite() throws Exception {
        open(config());
        for (int i = 0; i < 10; i++) {
            log.appendAsLeader(batch("v" + i), 0, -1);
        }
        log.closeAbruptly();
        Path active = log.segments().stream().reduce((a, b) -> b).orElseThrow().logFile();
        log = null;
        try (RandomAccessFile raf = new RandomAccessFile(active.toFile(), "rw")) {
            raf.setLength(raf.length() - 7);
        }

        open(config());
        assertThat(log.logEndOffset()).isEqualTo(9);
        assertThat(readAll(log, 0)).hasSize(9);
        log.appendAsLeader(batch("new"), 0, -1);
        assertThat(str(readAll(log, 9).get(0).value())).isEqualTo("new");
    }

    @Test
    void recoveryStopsAtCorruptBatch() throws Exception {
        open(config());
        int[] sizes = new int[5];
        for (int i = 0; i < 5; i++) {
            RecordBatch b = batch("value-" + i);
            sizes[i] = b.sizeInBytes();
            log.appendAsLeader(b, 0, -1);
        }
        log.closeAbruptly();
        Path file = log.segments().iterator().next().logFile();
        log = null;
        long corruptAt = sizes[0] + sizes[1] + sizes[2] - 3;
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.seek(corruptAt);
            int b = raf.read();
            raf.seek(corruptAt);
            raf.write(b ^ 0xff);
        }

        open(config());
        assertThat(log.logEndOffset()).isEqualTo(2);
        assertThat(Files.size(file)).isEqualTo(sizes[0] + sizes[1]);
    }

    @Test
    void uncleanShutdownDropsGarbageAfterValidData() throws Exception {
        open(config());
        log.appendAsLeader(batch("x"), 0, -1);
        log.closeAbruptly();
        Path file = log.segments().iterator().next().logFile();
        log = null;
        Files.write(file, new byte[100], java.nio.file.StandardOpenOption.APPEND);

        open(config());
        assertThat(log.logEndOffset()).isEqualTo(1);
    }

    @Test
    void sizeRetentionDeletesOldestClosedSegments() throws Exception {
        open(config(LogConfig.RETENTION_BYTES, "8192"));
        for (int i = 0; i < 40; i++) {
            log.appendAsLeader(sized(1, 900), 0, -1);
        }
        int before = log.numSegments();
        int deleted = log.deleteOldSegments(log.logEndOffset());
        assertThat(deleted).isPositive();
        assertThat(log.numSegments()).isEqualTo(before - deleted);
        assertThat(log.size()).isLessThanOrEqualTo(8192 + 4096);
        assertThat(log.logStartOffset()).isPositive();
        assertThatThrownBy(() -> log.read(0, 100, Long.MAX_VALUE)).isInstanceOf(OffsetOutOfRangeException.class);
    }

    @Test
    void timeRetentionUsesHybridTimestamps() throws Exception {
        open(config(LogConfig.RETENTION_MS, "60000"));
        long now = time.milliseconds();
        for (int i = 0; i < 20; i++) {
            log.appendAsLeader(sized(1, 900), 0, HybridTime.fromMillis(now));
        }
        assertThat(log.deleteOldSegments(log.logEndOffset())).isZero();
        time.advance(61_000);
        assertThat(log.deleteOldSegments(log.logEndOffset())).isPositive();
        assertThat(log.numSegments()).isEqualTo(1);
    }

    @Test
    void retentionNeverPassesUpperBound() throws Exception {
        open(config(LogConfig.RETENTION_BYTES, "0"));
        for (int i = 0; i < 20; i++) {
            log.appendAsLeader(sized(1, 900), 0, -1);
        }
        log.deleteOldSegments(0);
        assertThat(log.logStartOffset()).isZero();
    }

    @Test
    void truncateToRemovesSuffixAndEpochs() throws Exception {
        open(config());
        for (int i = 0; i < 50; i++) {
            log.appendAsLeader(batch("e1-" + i), 1, -1);
        }
        for (int i = 0; i < 50; i++) {
            log.appendAsLeader(batch("e2-" + i), 2, -1);
        }
        assertThat(log.endOffsetForEpoch(1)).isEqualTo(new OffsetAndEpoch(50, 1));

        log.truncateTo(30);
        assertThat(log.logEndOffset()).isEqualTo(30);
        assertThat(log.latestEpoch()).isEqualTo(1);
        assertThat(readAll(log, 0)).hasSize(30);

        LogAppendInfo info = log.appendAsLeader(batch("after"), 3, -1);
        assertThat(info.firstOffset()).isEqualTo(30);
        log.close();
        log = null;
        open(config());
        assertThat(log.logEndOffset()).isEqualTo(31);
    }

    @Test
    void truncateToStartOfLog() throws Exception {
        open(config());
        for (int i = 0; i < 10; i++) {
            log.appendAsLeader(batch("x"), 0, -1);
        }
        log.truncateTo(0);
        assertThat(log.logEndOffset()).isZero();
        assertThat(log.latestEpoch()).isEqualTo(-1);
    }

    @Test
    void followerAppendRequiresContiguousOffsets() throws Exception {
        open(config());
        RecordBatch b = batch("a", "b");
        b.setBaseOffset(0);
        b.setLeaderEpoch(5);
        log.appendAsFollower(b);
        RecordBatch gap = batch("c");
        gap.setBaseOffset(7);
        assertThatThrownBy(() -> log.appendAsFollower(gap)).isInstanceOf(IllegalStateException.class);
        assertThat(log.latestEpoch()).isEqualTo(5);
    }

    @Test
    void timestampsStayMonotonicEvenIfCallerClockGoesBack() throws Exception {
        open(config());
        log.appendAsLeader(batch("a", "b"), 0, HybridTime.pack(5_000, 0));
        LogAppendInfo info = log.appendAsLeader(batch("c"), 0, HybridTime.pack(4_000, 0));
        assertThat(info.maxTimestamp()).isEqualTo(HybridTime.pack(5_000, 2));
    }

    @Test
    void offsetForTimestamp() throws Exception {
        open(config());
        for (int i = 0; i < 100; i++) {
            log.appendAsLeader(batch("a", "b"), 0, HybridTime.pack(10_000 + i * 10L, 0));
        }
        TimestampAndOffset hit = log.offsetForTimestamp(HybridTime.pack(10_500, 0), Long.MAX_VALUE);
        assertThat(hit.offset()).isEqualTo(100);
        TimestampAndOffset mid = log.offsetForTimestamp(HybridTime.pack(10_500, 1), Long.MAX_VALUE);
        assertThat(mid.offset()).isEqualTo(101);
        TimestampAndOffset between = log.offsetForTimestamp(HybridTime.pack(10_505, 0), Long.MAX_VALUE);
        assertThat(between.offset()).isEqualTo(102);
        assertThat(log.offsetForTimestamp(HybridTime.pack(0, 0), Long.MAX_VALUE).offset()).isZero();
        assertThat(log.offsetForTimestamp(HybridTime.pack(99_999, 0), Long.MAX_VALUE)).isNull();
        assertThat(log.offsetForTimestamp(HybridTime.pack(10_500, 0), 50)).isNull();
    }

    @Test
    void idempotentProducerDuplicatesAreDetected() throws Exception {
        open(config());
        log.appendAsLeader(idempotent(42, 0, "a", "b"), 0, -1);
        log.appendAsLeader(idempotent(42, 2, "c"), 0, -1);
        LogAppendInfo dup = log.appendAsLeader(idempotent(42, 0, "a", "b"), 0, -1);
        assertThat(dup.duplicate()).isTrue();
        assertThat(dup.firstOffset()).isZero();
        assertThat(log.logEndOffset()).isEqualTo(3);
        assertThatThrownBy(() -> log.appendAsLeader(idempotent(42, 5, "x"), 0, -1))
                .isInstanceOf(OutOfOrderSequenceException.class);
        assertThatThrownBy(() -> log.appendAsLeader(idempotent(43, 3, "x"), 0, -1))
                .isInstanceOf(UnknownProducerIdException.class);

        log.close();
        log = null;
        open(config());
        assertThat(log.appendAsLeader(idempotent(42, 2, "c"), 0, -1).duplicate()).isTrue();
        assertThat(log.appendAsLeader(idempotent(42, 3, "d"), 0, -1).firstOffset()).isEqualTo(3);
    }

    @Test
    void flushAdvancesRecoveryPoint() throws Exception {
        open(config());
        for (int i = 0; i < 30; i++) {
            log.appendAsLeader(sized(1, 500), 0, -1);
        }
        assertThat(log.flush()).isEqualTo(30);
        assertThat(log.recoveryPoint()).isEqualTo(30);
        log.truncateTo(10);
        assertThat(log.recoveryPoint()).isEqualTo(10);
    }

    @Test
    void rejectsOversizedBatch() throws Exception {
        open(config(LogConfig.MAX_MESSAGE_BYTES, "1000"));
        assertThatThrownBy(() -> log.appendAsLeader(sized(1, 2000), 0, -1))
                .isInstanceOf(RecordTooLargeException.class);
    }
}
