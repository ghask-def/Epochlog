package io.epochlog.broker;

import io.epochlog.common.AtomicFiles;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Hands out producer ids that are unique across brokers and restarts: the broker id goes in the
 * high bits, and the low bits come from blocks reserved in a checkpoint file before use.
 *
 * <p>Thread-safety: thread-safe (synchronized).
 */
final class ProducerIdManager {
    private static final int BLOCK_SIZE = 1000;
    private static final int BROKER_SHIFT = 40;

    private final Path file;
    private final long brokerBits;
    private long next;
    private long blockEnd;

    ProducerIdManager(Path dataDir, int brokerId) {
        this.file = dataDir.resolve("producer-id-block");
        this.brokerBits = (long) brokerId << BROKER_SHIFT;
    }

    synchronized long nextProducerId() throws IOException {
        if (next >= blockEnd) {
            long start = Files.exists(file) ? Long.parseLong(Files.readString(file).trim()) : 0L;
            AtomicFiles.write(file, Long.toString(start + BLOCK_SIZE));
            next = start;
            blockEnd = start + BLOCK_SIZE;
        }
        return brokerBits | next++;
    }
}
