package io.epochlog.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class GroupCommitterTest {

    /** Pretends each fsync takes a while, like a real disk. */
    static final class SlowFile implements GroupCommitter.Syncable {
        final AtomicLong written = new AtomicLong();
        final AtomicInteger syncs = new AtomicInteger();

        long write() {
            return written.incrementAndGet();
        }

        @Override
        public long sync() {
            long target = written.get();
            syncs.incrementAndGet();
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return target;
        }
    }

    @Test
    void coalescesConcurrentCommits() throws Exception {
        SlowFile file = new SlowFile();
        try (GroupCommitter gc = new GroupCommitter("gc", 2, TimeUnit.MILLISECONDS, 512)) {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            List<Thread> writers = new ArrayList<>();
            for (int t = 0; t < 8; t++) {
                Thread th = new Thread(() -> {
                    for (int i = 0; i < 500; i++) {
                        long off = file.write();
                        CompletableFuture<Void> f = gc.commit(file, off);
                        synchronized (futures) {
                            futures.add(f);
                        }
                    }
                });
                th.start();
                writers.add(th);
            }
            for (Thread th : writers) {
                th.join();
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).get(30, TimeUnit.SECONDS);
            assertThat(gc.commitCount()).isEqualTo(4000);
            assertThat(file.syncs.get()).isLessThan(4000 / 4);
        }
    }

    @Test
    void failedSyncFailsWaiters() throws Exception {
        GroupCommitter.Syncable broken = () -> {
            throw new IOException("disk on fire");
        };
        try (GroupCommitter gc = new GroupCommitter("gc", 0, TimeUnit.MILLISECONDS, 16)) {
            CompletableFuture<Void> f = gc.commit(broken, 1);
            assertThatThrownBy(() -> f.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasRootCauseMessage("disk on fire");
        }
    }

    @Test
    void zeroWaitStillCompletes() throws Exception {
        SlowFile file = new SlowFile();
        try (GroupCommitter gc = new GroupCommitter("gc", 0, TimeUnit.MILLISECONDS, 1)) {
            for (int i = 0; i < 50; i++) {
                gc.commit(file, file.write()).get(5, TimeUnit.SECONDS);
            }
            assertThat(file.syncs.get()).isEqualTo(50);
        }
    }
}
