package io.epochlog.concurrent;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ExecutorsTest {

    @Test
    void singleWriterRunsEverythingOnOneThreadAndCallsHook() throws Exception {
        AtomicInteger batches = new AtomicInteger();
        List<String> threadNames = Collections.synchronizedList(new ArrayList<>());
        int[] counter = {0};
        int producers = 4;
        int each = 50_000;
        CountDownLatch done = new CountDownLatch(producers * each);
        try (SingleWriterExecutor ex = new SingleWriterExecutor("writer", 64, batches::incrementAndGet)) {
            List<Thread> threads = new ArrayList<>();
            for (int p = 0; p < producers; p++) {
                Thread t = new Thread(() -> {
                    for (int i = 0; i < each; i++) {
                        ex.execute(() -> {
                            counter[0]++;
                            if (counter[0] == 1) {
                                threadNames.add(Thread.currentThread().getName());
                            }
                            done.countDown();
                        });
                    }
                });
                t.start();
                threads.add(t);
            }
            assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
            for (Thread t : threads) {
                t.join();
            }
            CountDownLatch read = new CountDownLatch(1);
            int[] seen = new int[1];
            ex.execute(() -> {
                seen[0] = counter[0];
                read.countDown();
            });
            read.await();
            assertThat(seen[0]).isEqualTo(producers * each);
        }
        assertThat(threadNames).containsExactly("writer");
        assertThat(batches.get()).isPositive();
    }

    @Test
    void singleWriterWakesUpAfterIdle() throws Exception {
        try (SingleWriterExecutor ex = new SingleWriterExecutor("idle")) {
            for (int i = 0; i < 20; i++) {
                CountDownLatch l = new CountDownLatch(1);
                Thread.sleep(2);
                long start = System.nanoTime();
                ex.execute(l::countDown);
                assertThat(l.await(1, TimeUnit.SECONDS)).isTrue();
                assertThat(System.nanoTime() - start).isLessThan(TimeUnit.MILLISECONDS.toNanos(50));
            }
        }
    }

    @Test
    void serialExecutorPreservesOrderOnSharedPool() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<SerialExecutor> serials = new ArrayList<>();
            List<List<Integer>> seen = new ArrayList<>();
            AtomicInteger[] active = new AtomicInteger[10];
            AtomicInteger overlaps = new AtomicInteger();
            for (int s = 0; s < 10; s++) {
                serials.add(new SerialExecutor(pool));
                seen.add(Collections.synchronizedList(new ArrayList<>()));
                active[s] = new AtomicInteger();
            }
            CountDownLatch done = new CountDownLatch(10 * 1000);
            for (int i = 0; i < 1000; i++) {
                for (int s = 0; s < 10; s++) {
                    int task = i;
                    int serial = s;
                    serials.get(s).execute(() -> {
                        if (active[serial].incrementAndGet() > 1) {
                            overlaps.incrementAndGet();
                        }
                        seen.get(serial).add(task);
                        active[serial].decrementAndGet();
                        done.countDown();
                    });
                }
            }
            assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(overlaps.get()).isZero();
            for (List<Integer> l : seen) {
                assertThat(l).isSorted().hasSize(1000);
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
