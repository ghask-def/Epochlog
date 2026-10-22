package io.epochlog.storage;

import io.epochlog.common.AtomicFiles;
import io.epochlog.common.HybridTime;
import io.epochlog.common.RecordBatch;
import io.epochlog.common.Time;
import io.epochlog.common.TopicPartition;
import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A partition's log: an ordered set of {@link LogSegment}s, the leader epoch history and the
 * idempotent producer state.
 *
 * <p>Thread-safety: single-writer. Appends, truncation and roll happen on the partition's
 * writer thread. Reads, timestamp lookups and {@link #flush()} may run on other threads. The
 * writer publishes new data by writing the volatile {@code logEndOffset} after the segment
 * write; a reader that sees offset {@code n} sees every byte of batches below {@code n}.
 * Structural changes (roll, truncation, retention, compaction swap) hold an internal lock so the
 * background retention and cleaner threads don't race the writer.
 */
public final class Log implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Log.class);

    public static final String CLEAN_SHUTDOWN_FILE = ".clean-shutdown";
    static final String RECOVERY_POINT_FILE = "recovery-point";
    static final String LOG_START_FILE = "log-start-offset";
    static final String CLEANED_SUFFIX = ".cleaned";
    static final String SWAP_SUFFIX = ".swap";

    private final Path dir;
    private final TopicPartition topicPartition;
    private final Time time;
    private volatile LogConfig config;

    private final ConcurrentSkipListMap<Long, LogSegment> segments = new ConcurrentSkipListMap<>();
    private final LeaderEpochCache epochCache;
    private final ProducerStateManager producerState = new ProducerStateManager();
    private final ReentrantLock lock = new ReentrantLock();

    private volatile LogSegment active;
    private volatile long logEndOffset;
    private volatile long logStartOffset;
    private volatile long recoveryPoint;
    private volatile long lastTimestamp = HybridTime.NO_TIMESTAMP;
    private long truncations;
    private boolean closed;

    private Log(Path dir, TopicPartition tp, LogConfig config, Time time) throws IOException {
        this.dir = dir;
        this.topicPartition = tp;
        this.config = config;
        this.time = time;
        this.epochCache = new LeaderEpochCache(dir.resolve(LeaderEpochCache.FILE_NAME));
    }

    public static Log open(Path dir, TopicPartition tp, LogConfig config, Time time) throws IOException {
        Files.createDirectories(dir);
        Log log = new Log(dir, tp, config, time);
        log.load();
        return log;
    }

    private void load() throws IOException {
        Set<Long> forceRecovery = cleanupLeftovers();
        Path marker = dir.resolve(CLEAN_SHUTDOWN_FILE);
        boolean clean = Files.exists(marker);
        Files.deleteIfExists(marker);
        long savedRecoveryPoint = readCheckpoint(RECOVERY_POINT_FILE, 0L);
        long savedStart = readCheckpoint(LOG_START_FILE, -1L);

        List<Long> bases;
        try (Stream<Path> files = Files.list(dir)) {
            bases = files.filter(p -> p.getFileName().toString().endsWith(LogSegment.LOG_SUFFIX))
                    .map(LogSegment::parseBaseOffset)
                    .sorted()
                    .toList();
        }

        long now = time.milliseconds();
        int recovered = 0;
        for (int i = 0; i < bases.size(); i++) {
            long base = bases.get(i);
            boolean last = i == bases.size() - 1;
            LogSegment seg = LogSegment.open(dir, base, config, now, last);
            segments.put(base, seg);
            boolean pastRecoveryPoint = last || bases.get(i + 1) > savedRecoveryPoint;
            boolean recover = forceRecovery.contains(base) || (!clean && pastRecoveryPoint);
            if (!recover) {
                if (seg.indexesLookSane()) {
                    seg.loadTail();
                } else {
                    LOG.warn("{}: index for segment {} looks corrupt, rebuilding", topicPartition, base);
                    recover = true;
                }
            }
            if (recover) {
                recovered++;
                int truncated = seg.recover(b -> epochCache.assign(b.leaderEpoch(), b.baseOffset()));
                if (truncated > 0) {
                    LOG.warn("{}: truncated {} bytes of torn or corrupt data from segment {}",
                            topicPartition, truncated, base);
                    for (int j = i + 1; j < bases.size(); j++) {
                        LogSegment.open(dir, bases.get(j), config, now, false).delete();
                    }
                    break;
                }
                if (!last) {
                    seg.onBecomeInactive();
                }
            }
        }

        if (segments.isEmpty()) {
            long start = Math.max(savedStart, 0L);
            segments.put(start, LogSegment.open(dir, start, config, now, true));
        }
        LogSegment last = segments.lastEntry().getValue();
        if (!bases.isEmpty() && last.baseOffset() != bases.get(bases.size() - 1)) {
            // A torn write deleted the segments after this one, so it was opened read-only.
            last.makeWritable();
        }
        active = last;
        logEndOffset = last.nextOffset();
        logStartOffset = Math.max(savedStart, segments.firstKey());
        for (LogSegment seg : segments.values()) {
            if (seg.maxTimestamp() > lastTimestamp) {
                lastTimestamp = seg.maxTimestamp();
            }
        }
        if (recovered > 0) {
            for (LogSegment seg : segments.tailMap(segments.floorKey(Math.min(savedRecoveryPoint, logEndOffset))).values()) {
                seg.flush();
            }
        }
        recoveryPoint = logEndOffset;
        epochCache.truncateFromEnd(logEndOffset);
        epochCache.truncateFromStart(logStartOffset);
        rebuildProducerState();
        if (bases.isEmpty()) {
            LOG.info("{}: created new log at offset {}", topicPartition, logEndOffset);
        } else {
            LOG.info("{}: loaded {} segments, start={}, end={}, recovered={}, clean shutdown={}", topicPartition,
                    segments.size(), logStartOffset, logEndOffset, recovered, clean);
        }
    }

    private List<Long> listBases() {
        return new ArrayList<>(segments.keySet());
    }

    /** Deletes half-written compaction output and finishes any swap that was interrupted. */
    private Set<Long> cleanupLeftovers() throws IOException {
        Set<Long> swapped = new HashSet<>();
        List<Path> files;
        try (Stream<Path> s = Files.list(dir)) {
            files = s.sorted(Comparator.comparing(Path::toString)).toList();
        }
        Set<String> completeSwaps = new HashSet<>();
        for (Path p : files) {
            String name = p.getFileName().toString();
            if (name.endsWith(LogSegment.LOG_SUFFIX + SWAP_SUFFIX)) {
                completeSwaps.add(name.substring(0, name.indexOf('.')));
            }
        }
        for (Path p : files) {
            String name = p.getFileName().toString();
            if (name.endsWith(CLEANED_SUFFIX) || name.endsWith(".tmp") || name.endsWith(".deleted")) {
                Files.deleteIfExists(p);
            } else if (name.endsWith(SWAP_SUFFIX)) {
                String prefix = name.substring(0, name.indexOf('.'));
                if (!completeSwaps.contains(prefix)) {
                    Files.deleteIfExists(p);
                } else if (name.endsWith(LogSegment.LOG_SUFFIX + SWAP_SUFFIX)) {
                    Path target = p.resolveSibling(name.substring(0, name.length() - SWAP_SUFFIX.length()));
                    Files.move(p, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                            java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                    Files.deleteIfExists(p.resolveSibling(prefix + LogSegment.INDEX_SUFFIX));
                    Files.deleteIfExists(p.resolveSibling(prefix + LogSegment.TIME_INDEX_SUFFIX));
                    swapped.add(Long.parseLong(prefix));
                    LOG.info("{}: completed interrupted segment swap for {}", topicPartition, prefix);
                } else {
                    Files.deleteIfExists(p);
                }
            }
        }
        return swapped;
    }

    private long readCheckpoint(String name, long def) throws IOException {
        Path p = dir.resolve(name);
        if (!Files.exists(p)) {
            return def;
        }
        String s = Files.readString(p).trim();
        return s.isEmpty() ? def : Long.parseLong(s);
    }

    private void rebuildProducerState() throws IOException {
        producerState.clear();
        for (LogSegment seg : segments.values()) {
            seg.forEachBatchHeader(0, producerState::update);
        }
    }

    /**
     * Appends a batch as leader: checks idempotence, assigns offsets and the leader epoch, and
     * stamps an HLC timestamp. The stamp is bumped if needed so timestamps never go backwards in
     * this log, even if the caller's clock is behind the previous leader's.
     *
     * @param timestamp HLC base timestamp to stamp, or a negative value to keep the batch's own
     */
    public LogAppendInfo appendAsLeader(RecordBatch batch, int leaderEpoch, long timestamp) throws IOException {
        batch.ensureValid();
        checkSize(batch);
        ProducerStateManager.Check check = producerState.check(batch);
        if (check.verdict() == ProducerStateManager.Verdict.DUPLICATE) {
            ProducerStateManager.BatchMetadata m = check.original();
            return new LogAppendInfo(m.firstOffset(), m.lastOffset(), m.maxTimestamp(), true);
        }
        if (timestamp >= 0) {
            long last = lastTimestamp;
            batch.setBaseTimestamp(last >= 0 && timestamp <= last ? last + 1 : timestamp);
        }
        long base = logEndOffset;
        batch.setBaseOffset(base);
        batch.setLeaderEpoch(leaderEpoch);
        if (leaderEpoch > epochCache.latestEpoch()) {
            epochCache.assign(leaderEpoch, base);
        }
        return doAppend(batch);
    }

    /** Appends a batch replicated from the leader. Offsets must continue exactly from the log end. */
    public LogAppendInfo appendAsFollower(RecordBatch batch) throws IOException {
        batch.ensureValid();
        if (batch.baseOffset() != logEndOffset) {
            throw new IllegalStateException(topicPartition + ": follower append at " + batch.baseOffset()
                    + " but log end is " + logEndOffset);
        }
        if (batch.leaderEpoch() > epochCache.latestEpoch()) {
            epochCache.assign(batch.leaderEpoch(), batch.baseOffset());
        }
        return doAppend(batch);
    }

    private LogAppendInfo doAppend(RecordBatch batch) throws IOException {
        if (active.shouldRoll(batch.sizeInBytes(), time.milliseconds())) {
            roll();
        }
        active.append(batch);
        producerState.update(batch);
        if (batch.maxTimestamp() > lastTimestamp) {
            lastTimestamp = batch.maxTimestamp();
        }
        logEndOffset = batch.nextOffset();
        return new LogAppendInfo(batch.baseOffset(), batch.lastOffset(), batch.maxTimestamp(), false);
    }

    private void checkSize(RecordBatch batch) {
        if (batch.sizeInBytes() > config.maxBatchBytes()) {
            throw new RecordTooLargeException("batch of " + batch.sizeInBytes() + " bytes exceeds "
                    + config.maxBatchBytes());
        }
    }

    /** Starts a new active segment at the log end offset. No-op if the active segment is empty. */
    public void roll() throws IOException {
        lock.lock();
        try {
            LogSegment current = active;
            long base = logEndOffset;
            if (current.size() == 0 || current.baseOffset() == base) {
                return;
            }
            current.onBecomeInactive();
            LogSegment next = LogSegment.open(dir, base, config, time.milliseconds(), true);
            segments.put(base, next);
            active = next;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reads whole batches containing offsets in {@code [startOffset, maxOffset)}. The first
     * batch may begin before {@code startOffset}; callers filter records themselves.
     *
     * @return the slice, or null if there is nothing to read below {@code maxOffset}
     * @throws OffsetOutOfRangeException if the offset is before the log start or after the end
     */
    public FileSlice read(long startOffset, int maxBytes, long maxOffset) throws IOException {
        for (int attempt = 0; ; attempt++) {
            long end = logEndOffset;
            long start = logStartOffset;
            if (startOffset < start || startOffset > end) {
                throw new OffsetOutOfRangeException(topicPartition + ": offset " + startOffset
                        + " outside [" + start + ", " + end + "]");
            }
            long limit = Math.min(maxOffset, end);
            if (startOffset >= limit) {
                return null;
            }
            try {
                Map.Entry<Long, LogSegment> e = segments.floorEntry(startOffset);
                while (e != null) {
                    FileSlice slice = e.getValue().read(startOffset, maxBytes, limit);
                    if (slice != null) {
                        return slice;
                    }
                    e = segments.higherEntry(e.getKey());
                }
                return null;
            } catch (ClosedChannelException ex) {
                // The segment was deleted or replaced under us; the map now has its successor.
                if (attempt >= 3) {
                    throw ex;
                }
            }
        }
    }

    /** First record with HLC timestamp {@code >= target} and offset below {@code maxOffset}, or null. */
    public TimestampAndOffset offsetForTimestamp(long target, long maxOffset) throws IOException {
        for (LogSegment seg : segments.values()) {
            if (seg.maxTimestamp() < target) {
                continue;
            }
            TimestampAndOffset found = seg.findOffsetByTimestamp(target, logStartOffset);
            if (found != null) {
                return found.offset() < maxOffset ? found : null;
            }
        }
        return null;
    }

    /**
     * Fsyncs everything appended so far.
     *
     * @return the offset below which all data is durable
     */
    public long flush() throws IOException {
        long target;
        long truncationsSeen;
        lock.lock();
        try {
            target = logEndOffset;
            truncationsSeen = truncations;
        } finally {
            lock.unlock();
        }
        long rp = recoveryPoint;
        if (target <= rp) {
            return rp;
        }
        Long from = segments.floorKey(rp);
        for (LogSegment seg : (from == null ? segments : segments.tailMap(from)).values()) {
            if (seg.baseOffset() >= target) {
                break;
            }
            try {
                seg.flush();
            } catch (ClosedChannelException e) {
                if (seg == active) {
                    throw e;
                }
            }
        }
        lock.lock();
        try {
            if (truncations == truncationsSeen && target > recoveryPoint) {
                recoveryPoint = target;
            }
            return recoveryPoint;
        } finally {
            lock.unlock();
        }
    }

    public void checkpointRecoveryPoint() throws IOException {
        AtomicFiles.write(dir.resolve(RECOVERY_POINT_FILE), Long.toString(recoveryPoint));
    }

    /**
     * Removes every batch with an offset {@code >= offset}. Used when a follower's log diverges
     * from its leader's.
     *
     * @return true if anything was removed
     */
    public boolean truncateTo(long offset) throws IOException {
        lock.lock();
        try {
            if (offset >= logEndOffset) {
                return false;
            }
            if (offset <= segments.firstKey()) {
                truncateFullyAndStartAt(offset);
                return true;
            }
            LOG.info("{}: truncating from {} to {}", topicPartition, logEndOffset, offset);
            for (LogSegment seg : new ArrayList<>(segments.tailMap(offset, true).values())) {
                segments.remove(seg.baseOffset());
                seg.delete();
            }
            LogSegment last = segments.lastEntry().getValue();
            last.truncateTo(offset);
            if (last != active) {
                last.makeWritable();
                active = last;
            }
            logEndOffset = last.nextOffset();
            recoveryPoint = Math.min(recoveryPoint, logEndOffset);
            truncations++;
            lastTimestamp = HybridTime.NO_TIMESTAMP;
            for (LogSegment seg : segments.values()) {
                lastTimestamp = Math.max(lastTimestamp, seg.maxTimestamp());
            }
            epochCache.truncateFromEnd(logEndOffset);
            rebuildProducerState();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Throws away the whole log and restarts it empty at {@code offset}. */
    public void truncateFullyAndStartAt(long offset) throws IOException {
        lock.lock();
        try {
            LOG.info("{}: truncating fully and restarting at {}", topicPartition, offset);
            for (LogSegment seg : segments.values()) {
                seg.delete();
            }
            segments.clear();
            LogSegment fresh = LogSegment.open(dir, offset, config, time.milliseconds(), true);
            segments.put(offset, fresh);
            active = fresh;
            logEndOffset = offset;
            logStartOffset = offset;
            recoveryPoint = offset;
            lastTimestamp = HybridTime.NO_TIMESTAMP;
            truncations++;
            epochCache.clear();
            producerState.clear();
            AtomicFiles.write(dir.resolve(LOG_START_FILE), Long.toString(offset));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Applies time and size retention to closed segments that end at or below
     * {@code upperBoundOffset} (normally the high watermark).
     *
     * @return number of segments deleted
     */
    public int deleteOldSegments(long upperBoundOffset) throws IOException {
        lock.lock();
        try {
            LogConfig cfg = config;
            if (cfg.compact()) {
                return 0;
            }
            long now = time.milliseconds();
            long total = size();
            List<LogSegment> doomed = new ArrayList<>();
            for (LogSegment seg : segments.values()) {
                if (seg == active || seg.nextOffset() > upperBoundOffset) {
                    break;
                }
                boolean expired = cfg.retentionMs() >= 0 && lastModifiedMillis(seg) < now - cfg.retentionMs();
                boolean oversize = cfg.retentionBytes() >= 0 && total - seg.size() >= cfg.retentionBytes();
                if (!expired && !oversize) {
                    break;
                }
                total -= seg.size();
                doomed.add(seg);
            }
            if (doomed.isEmpty()) {
                return 0;
            }
            for (LogSegment seg : doomed) {
                segments.remove(seg.baseOffset());
            }
            logStartOffset = Math.max(logStartOffset, segments.firstKey());
            AtomicFiles.write(dir.resolve(LOG_START_FILE), Long.toString(logStartOffset));
            epochCache.truncateFromStart(logStartOffset);
            for (LogSegment seg : doomed) {
                seg.delete();
            }
            LOG.info("{}: retention deleted {} segments, log start now {}", topicPartition, doomed.size(),
                    logStartOffset);
            return doomed.size();
        } finally {
            lock.unlock();
        }
    }

    private static long lastModifiedMillis(LogSegment seg) throws IOException {
        long ts = seg.maxTimestamp();
        if (ts >= 0) {
            return HybridTime.physicalMillis(ts);
        }
        return Files.getLastModifiedTime(seg.logFile()).toMillis();
    }

    /** Closed segments eligible for compaction, oldest first. */
    public List<LogSegment> closedSegments() {
        List<LogSegment> out = new ArrayList<>();
        for (LogSegment seg : segments.values()) {
            if (seg == active) {
                break;
            }
            out.add(seg);
        }
        return out;
    }

    /** Creates an empty segment for compaction output alongside {@code base}. */
    LogSegment newCleanedSegment(long base) throws IOException {
        return LogSegment.openWithSuffix(dir, base, config, time.milliseconds(), CLEANED_SUFFIX);
    }

    /**
     * Atomically replaces a closed segment with its compacted copy. The copy is renamed to
     * {@code .swap} first, so a crash at any point either keeps the old segment or finishes the
     * swap on the next load.
     *
     * @return false if the original was already removed or replaced
     */
    boolean replaceSegment(LogSegment original, LogSegment cleaned) throws IOException {
        lock.lock();
        try {
            if (segments.get(original.baseOffset()) != original || original == active) {
                cleaned.delete();
                return false;
            }
            cleaned.onBecomeInactive();
            cleaned.changeSuffix(CLEANED_SUFFIX, SWAP_SUFFIX);
            AtomicFiles.fsyncDirectory(dir);
            segments.put(original.baseOffset(), cleaned);
            original.delete();
            cleaned.changeSuffix(SWAP_SUFFIX, "");
            AtomicFiles.fsyncDirectory(dir);
            return true;
        } finally {
            lock.unlock();
        }
    }

    public long logEndOffset() {
        return logEndOffset;
    }

    public long logStartOffset() {
        return logStartOffset;
    }

    public long recoveryPoint() {
        return recoveryPoint;
    }

    /** Largest HLC timestamp in the log, or -1. */
    public long lastTimestamp() {
        return lastTimestamp;
    }

    public int latestEpoch() {
        return epochCache.latestEpoch();
    }

    public OffsetAndEpoch endOffsetForEpoch(int epoch) {
        return epochCache.endOffsetFor(epoch, logEndOffset);
    }

    public LeaderEpochCache epochCache() {
        return epochCache;
    }

    /** Writer-thread only. */
    public ProducerStateManager producerState() {
        return producerState;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public Path dir() {
        return dir;
    }

    public LogConfig config() {
        return config;
    }

    public void updateConfig(LogConfig newConfig) {
        this.config = newConfig;
    }

    public long size() {
        long total = 0;
        for (LogSegment seg : segments.values()) {
            total += seg.size();
        }
        return total;
    }

    public int numSegments() {
        return segments.size();
    }

    public Collection<LogSegment> segments() {
        return List.copyOf(segments.values());
    }

    List<Long> segmentBases() {
        return listBases();
    }

    /** Flushes, checkpoints and writes the clean shutdown marker. */
    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            flush();
            for (LogSegment seg : segments.values()) {
                seg.close();
            }
            checkpointRecoveryPoint();
            AtomicFiles.write(dir.resolve(LOG_START_FILE), Long.toString(logStartOffset));
            AtomicFiles.write(dir.resolve(CLEAN_SHUTDOWN_FILE), "");
            closed = true;
        } finally {
            lock.unlock();
        }
    }

    /** Closes file handles without flushing or marking a clean shutdown, as if the process died. */
    public void closeAbruptly() throws IOException {
        lock.lock();
        try {
            closed = true;
            for (LogSegment seg : segments.values()) {
                seg.close();
            }
        } finally {
            lock.unlock();
        }
    }

    /** Closes and removes the partition directory. */
    public void delete() throws IOException {
        closeAbruptly();
        try (Stream<Path> walk = Files.walk(dir)) {
            for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(p);
            }
        }
    }

    @Override
    public String toString() {
        return "Log(" + topicPartition + ", start=" + logStartOffset + ", end=" + logEndOffset + ")";
    }
}
