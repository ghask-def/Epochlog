package io.epochlog.concurrent;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

class MpscArrayQueueTest {

    @Test
    void fifoAndCapacity() {
        MpscArrayQueue<Integer> q = new MpscArrayQueue<>(5);
        assertThat(q.capacity()).isEqualTo(8);
        for (int i = 0; i < 8; i++) {
            assertThat(q.offer(i)).isTrue();
        }
        assertThat(q.offer(99)).isFalse();
        assertThat(q.size()).isEqualTo(8);
        for (int i = 0; i < 8; i++) {
            assertThat(q.poll()).isEqualTo(i);
        }
        assertThat(q.poll()).isNull();
        assertThat(q.isEmpty()).isTrue();
        assertThat(q.offer(100)).isTrue();
        assertThat(q.poll()).isEqualTo(100);
    }

    @Test
    void manyProducersKeepPerProducerOrder() throws Exception {
        int producers = 4;
        int perProducer = 200_000;
        MpscArrayQueue<long[]> q = new MpscArrayQueue<>(1024);
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int p = 0; p < producers; p++) {
            int id = p;
            Thread t = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    return;
                }
                for (int i = 0; i < perProducer; i++) {
                    long[] item = {id, i};
                    while (!q.offer(item)) {
                        Thread.onSpinWait();
                    }
                }
            });
            t.start();
            threads.add(t);
        }
        start.countDown();
        long[] next = new long[producers];
        int received = 0;
        while (received < producers * perProducer) {
            long[] item = q.poll();
            if (item == null) {
                Thread.onSpinWait();
                continue;
            }
            assertThat(item[1]).isEqualTo(next[(int) item[0]]);
            next[(int) item[0]]++;
            received++;
        }
        for (Thread t : threads) {
            t.join();
        }
        assertThat(q.poll()).isNull();
    }
}
