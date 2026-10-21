package io.epochlog.bench;

import io.epochlog.common.Config;
import io.epochlog.common.HybridTime;
import io.epochlog.common.RecordBatch;
import io.epochlog.common.RecordBatchBuilder;
import io.epochlog.common.Time;
import io.epochlog.common.TopicPartition;
import io.epochlog.concurrent.GroupCommitter;
import io.epochlog.storage.Log;
import io.epochlog.storage.LogConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Append path cost: page-cache-only appends, an fsync after every append, and group commit
 * with several writers sharing the fsync. The batch is re-stamped each time, which also measures
 * the CRC recompute.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 3)
@Fork(1)
@State(Scope.Benchmark)
public class LogAppendBenchmark {

    @Param({"1024"})
    int batchBytes;

    Path dir;
    Log log;
    GroupCommitter committer;
    RecordBatch template;
    long tick;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        dir = Files.createTempDirectory("bench-log");
        log = Log.open(dir, new TopicPartition("bench", 0),
                LogConfig.from(Config.empty().with(LogConfig.SEGMENT_BYTES, 256 << 20)), Time.SYSTEM);
        committer = new GroupCommitter("bench-gc", 2, TimeUnit.MILLISECONDS, 1024);
        RecordBatchBuilder b = new RecordBatchBuilder();
        int recordSize = 100;
        for (int i = 0; i < batchBytes / recordSize; i++) {
            b.append(null, new byte[recordSize - 10]);
        }
        template = b.build();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        committer.close();
        log.close();
        try (Stream<Path> walk = Files.walk(dir)) {
            for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(p);
            }
        }
    }

    private RecordBatch nextBatch() {
        return RecordBatch.wrap(template.buffer());
    }

    @Benchmark
    public long appendPageCache() throws IOException {
        synchronized (this) {
            return log.appendAsLeader(nextBatch(), 0, HybridTime.pack(System.currentTimeMillis(), (int) (tick++ & 0xff))).lastOffset();
        }
    }

    @Benchmark
    public long appendFsyncEach() throws IOException {
        synchronized (this) {
            long off = log.appendAsLeader(nextBatch(), 0, -1).lastOffset();
            log.flush();
            return off;
        }
    }

    /** Eight writers; each waits for durability, but fsyncs are shared through the committer. */
    @Benchmark
    @org.openjdk.jmh.annotations.Threads(8)
    public void appendGroupCommit() throws Exception {
        long next;
        synchronized (this) {
            next = log.appendAsLeader(nextBatch(), 0, -1).lastOffset() + 1;
        }
        committer.commit(log::flush, next).get();
    }

    @Benchmark
    @org.openjdk.jmh.annotations.Threads(8)
    public void appendFsyncEachEightWriters() throws Exception {
        synchronized (this) {
            log.appendAsLeader(nextBatch(), 0, -1);
            log.flush();
        }
    }
}
