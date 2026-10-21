package io.epochlog.storage;

import io.epochlog.common.Record;
import io.epochlog.common.RecordBatch;
import io.epochlog.common.RecordBatchBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Key-based compaction. For each log with {@code cleanup.policy=compact}, builds a map of the
 * latest offset per key across the whole log and rewrites closed segments to keep only those
 * records. Offsets are preserved, so compacted batches have gaps. Records without a key are
 * kept, and batches from idempotent producers keep their header even when emptied so the
 * producer's sequence history survives a restart.
 *
 * <p>Thread-safety: thread-safe. Runs on its own background thread; segment swaps go through
 * {@link Log#replaceSegment}, which takes the log's structural lock.
 */
public final class LogCleaner implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(LogCleaner.class);

    private final Supplier<Collection<Log>> logs;
    private final long intervalMs;
    private final Thread thread;
    private volatile boolean running = true;

    public LogCleaner(Supplier<Collection<Log>> logs, long intervalMs) {
        this.logs = logs;
        this.intervalMs = intervalMs;
        this.thread = Thread.ofPlatform().name("log-cleaner").daemon().unstarted(this::run);
    }

    public void start() {
        thread.start();
    }

    private void run() {
        while (running) {
            for (Log log : logs.get()) {
                if (!running) {
                    return;
                }
                if (log.config().compact()) {
                    try {
                        clean(log);
                    } catch (IOException | RuntimeException e) {
                        LOG.warn("compaction of {} failed", log.topicPartition(), e);
                    }
                }
            }
            try {
                Thread.sleep(intervalMs);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    /**
     * Compacts all closed segments of {@code log}.
     *
     * @return number of records removed
     */
    public static long clean(Log log) throws IOException {
        List<LogSegment> closed = log.closedSegments();
        if (closed.isEmpty()) {
            return 0;
        }
        Map<ByteBuffer, Long> latest = new HashMap<>();
        for (LogSegment seg : log.segments()) {
            seg.forEachBatch(batch -> {
                for (Record r : batch.records()) {
                    if (r.key() != null) {
                        latest.put(ByteBuffer.wrap(r.key()), r.offset());
                    }
                }
            });
        }
        long removed = 0;
        for (LogSegment seg : closed) {
            LogSegment cleaned = log.newCleanedSegment(seg.baseOffset());
            long[] dropped = {0};
            try {
                seg.forEachBatch(batch -> {
                    List<Record> records = batch.records();
                    List<Record> keep = new ArrayList<>(records.size());
                    for (Record r : records) {
                        if (r.key() == null || latest.get(ByteBuffer.wrap(r.key())) == r.offset()) {
                            keep.add(r);
                        }
                    }
                    if (keep.size() == records.size()) {
                        cleaned.append(batch);
                        return;
                    }
                    dropped[0] += records.size() - keep.size();
                    if (keep.isEmpty() && !batch.hasProducerId()) {
                        return;
                    }
                    cleaned.append(rebuild(batch, keep));
                });
            } catch (IOException | RuntimeException e) {
                cleaned.delete();
                throw e;
            }
            if (dropped[0] == 0 || !log.replaceSegment(seg, cleaned)) {
                cleaned.delete();
                continue;
            }
            removed += dropped[0];
        }
        if (removed > 0) {
            LOG.info("{}: compaction removed {} records", log.topicPartition(), removed);
        }
        return removed;
    }

    private static RecordBatch rebuild(RecordBatch original, List<Record> keep) {
        RecordBatchBuilder b = new RecordBatchBuilder(original.compression(), original.baseOffset(),
                original.sizeInBytes())
                .producer(original.producerId(), original.baseSequence())
                .leaderEpoch(original.leaderEpoch())
                .baseTimestamp(original.baseTimestamp());
        for (Record r : keep) {
            b.appendWithOffset(r.offset(), r.key(), r.value(), r.headers());
        }
        b.lastOffset(original.lastOffset());
        return b.build();
    }

    @Override
    public void close() {
        running = false;
        thread.interrupt();
        try {
            thread.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
