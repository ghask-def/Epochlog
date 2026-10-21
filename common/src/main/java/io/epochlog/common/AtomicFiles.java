package io.epochlog.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * Crash-safe small file replacement: write a temp file, fsync it, rename over the target, then
 * fsync the directory.
 *
 * <p>Thread-safety: thread-safe for distinct targets; concurrent writers of one path must
 * coordinate externally.
 */
public final class AtomicFiles {
    private AtomicFiles() {}

    public static void write(Path target, String content) throws IOException {
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buf = ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8));
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            ch.force(true);
        }
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        fsyncDirectory(target.toAbsolutePath().getParent());
    }

    public static void fsyncDirectory(Path dir) {
        // Not every platform lets you open a directory for fsync. Where it fails there's nothing
        // better to do, so the rename is just less durable there.
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
        }
    }
}
