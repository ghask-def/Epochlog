package io.epochlog.storage;

import io.epochlog.common.AtomicFiles;
import io.epochlog.common.EpochlogException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The {@code (epoch, startOffset)} history of a partition, checkpointed to
 * {@code leader-epoch-checkpoint}. Followers use it after a leader change to find where their
 * log diverges from the new leader's.
 *
 * <p>Thread-safety: thread-safe. Mutations come from the partition writer; lookups may come
 * from request handler threads. A read-write lock guards the list.
 */
public final class LeaderEpochCache {
    public static final String FILE_NAME = "leader-epoch-checkpoint";
    private static final int VERSION = 0;

    /**
     * One epoch boundary.
     *
     * <p>Thread-safety: immutable.
     */
    public record EpochEntry(int epoch, long startOffset) {}

    private final Path file;
    private final List<EpochEntry> entries = new ArrayList<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public LeaderEpochCache(Path file) throws IOException {
        this.file = file;
        if (Files.exists(file)) {
            List<String> lines = Files.readAllLines(file);
            if (lines.size() < 2 || Integer.parseInt(lines.get(0).trim()) != VERSION) {
                throw new EpochlogException("bad leader epoch checkpoint " + file);
            }
            int n = Integer.parseInt(lines.get(1).trim());
            for (int i = 0; i < n; i++) {
                String[] parts = lines.get(2 + i).trim().split("\\s+");
                entries.add(new EpochEntry(Integer.parseInt(parts[0]), Long.parseLong(parts[1])));
            }
        }
    }

    /** Records that {@code epoch} starts at {@code startOffset}. Ignored unless the epoch is newer. */
    public void assign(int epoch, long startOffset) {
        if (epoch < 0) {
            return;
        }
        lock.writeLock().lock();
        try {
            if (!entries.isEmpty()) {
                EpochEntry last = entries.get(entries.size() - 1);
                if (epoch <= last.epoch()) {
                    return;
                }
                if (startOffset < last.startOffset()) {
                    throw new IllegalArgumentException("epoch " + epoch + " starts at " + startOffset
                            + ", before epoch " + last.epoch() + " at " + last.startOffset());
                }
            }
            entries.add(new EpochEntry(epoch, startOffset));
            persist();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int latestEpoch() {
        lock.readLock().lock();
        try {
            return entries.isEmpty() ? OffsetAndEpoch.UNDEFINED_EPOCH : entries.get(entries.size() - 1).epoch();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * End offset of {@code requestedEpoch}: the start offset of the next higher epoch, or the log
     * end offset if it is the latest. The returned epoch is the largest known epoch
     * {@code <= requestedEpoch}, which may be smaller than the one asked about.
     */
    public OffsetAndEpoch endOffsetFor(int requestedEpoch, long logEndOffset) {
        lock.readLock().lock();
        try {
            if (requestedEpoch == OffsetAndEpoch.UNDEFINED_EPOCH || entries.isEmpty()) {
                return OffsetAndEpoch.UNDEFINED;
            }
            EpochEntry latest = entries.get(entries.size() - 1);
            if (requestedEpoch == latest.epoch()) {
                return new OffsetAndEpoch(logEndOffset, latest.epoch());
            }
            if (requestedEpoch > latest.epoch()) {
                return OffsetAndEpoch.UNDEFINED;
            }
            EpochEntry floor = null;
            EpochEntry higher = null;
            for (EpochEntry e : entries) {
                if (e.epoch() <= requestedEpoch) {
                    floor = e;
                } else {
                    higher = e;
                    break;
                }
            }
            if (floor == null) {
                return new OffsetAndEpoch(higher.startOffset(), requestedEpoch);
            }
            return new OffsetAndEpoch(higher.startOffset(), floor.epoch());
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Drops entries starting at or after {@code endOffset}, after the log is truncated there. */
    public void truncateFromEnd(long endOffset) {
        lock.writeLock().lock();
        try {
            boolean changed = entries.removeIf(e -> e.startOffset() >= endOffset);
            if (changed) {
                persist();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Forgets epochs that ended before {@code startOffset}, after old segments are deleted. */
    public void truncateFromStart(long startOffset) {
        lock.writeLock().lock();
        try {
            int firstKept = 0;
            while (firstKept + 1 < entries.size() && entries.get(firstKept + 1).startOffset() <= startOffset) {
                firstKept++;
            }
            if (firstKept == 0 && (entries.isEmpty() || entries.get(0).startOffset() >= startOffset)) {
                return;
            }
            EpochEntry head = entries.get(firstKept);
            List<EpochEntry> kept = new ArrayList<>(entries.subList(firstKept + 1, entries.size()));
            entries.clear();
            entries.add(new EpochEntry(head.epoch(), Math.max(head.startOffset(), startOffset)));
            entries.addAll(kept);
            persist();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void clear() {
        lock.writeLock().lock();
        try {
            entries.clear();
            persist();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<EpochEntry> entries() {
        lock.readLock().lock();
        try {
            return List.copyOf(entries);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void persist() {
        StringBuilder sb = new StringBuilder();
        sb.append(VERSION).append('\n').append(entries.size()).append('\n');
        for (EpochEntry e : entries) {
            sb.append(e.epoch()).append(' ').append(e.startOffset()).append('\n');
        }
        try {
            AtomicFiles.write(file, sb.toString());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to write " + file, e);
        }
    }
}
