package io.epochlog.broker;

import io.epochlog.protocol.Records;
import io.epochlog.storage.FileSlice;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * Adapts a log {@link FileSlice} to the protocol's zero-copy records type.
 *
 * <p>Thread-safety: immutable.
 */
record SliceRecords(FileSlice slice) implements Records.Transferable {

    @Override
    public int sizeInBytes() {
        return slice.size();
    }

    @Override
    public long transferTo(WritableByteChannel target, long position, long count) throws IOException {
        return slice.transferTo(target, position, count);
    }
}
