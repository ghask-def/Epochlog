package io.epochlog.protocol;

import io.epochlog.common.RecordBatch;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 * Record batch bytes carried in a produce request or fetch response.
 *
 * <p>Thread-safety: implementations are immutable.
 */
public sealed interface Records permits MemoryRecords, Records.Transferable {

    int sizeInBytes();

    /**
     * Records that live somewhere else, typically a log segment, and are copied straight to the
     * socket when the response is written.
     *
     * <p>Thread-safety: implementations must allow {@link #transferTo} from the network thread.
     */
    non-sealed interface Transferable extends Records {
        long transferTo(WritableByteChannel target, long position, long count) throws IOException;
    }

    static MemoryRecords of(RecordBatch... batches) {
        return MemoryRecords.of(List.of(batches));
    }
}
