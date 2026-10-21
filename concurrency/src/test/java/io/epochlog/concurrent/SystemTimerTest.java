package io.epochlog.concurrent;

import static org.assertj.core.api.Assertions.assertThat;

import io.epochlog.common.ManualTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

class SystemTimerTest {

    private static final class Recorder extends TimerTask {
        final int id;
        final List<Integer> fired;

        Recorder(int id, long delay, List<Integer> fired) {
            super(delay);
            this.id = id;
            this.fired = fired;
        }

        @Override
        public void run() {
            fired.add(id);
        }
    }

    private static void advance(ManualTime time, SystemTimer timer, long ms) throws InterruptedException {
        for (long i = 0; i < ms; i++) {
            time.advance(1);
            while (timer.advanceClock(0)) {
                // drain everything due at this tick, including cascades from overflow wheels
            }
        }
    }

    @Test
    void firesEachTaskAtItsDeadlineAcrossOverflowWheels() throws Exception {
        ManualTime time = new ManualTime();
        Map<Integer, Long> firedAt = new ConcurrentHashMap<>();
        long start = time.monotonicMillis();
        try (SystemTimer timer = new SystemTimer(1, 8, time, Runnable::run)) {
            long[] delays = {1, 5, 7, 8, 9, 63, 64, 65, 500, 777, 3000};
            for (int i = 0; i < delays.length; i++) {
                int id = i;
                timer.add(new TimerTask(delays[i]) {
                    @Override
                    public void run() {
                        firedAt.put(id, time.monotonicMillis() - start);
                    }
                });
            }
            assertThat(timer.size()).isEqualTo(delays.length);
            advance(time, timer, 3100);
            assertThat(firedAt).hasSize(delays.length);
            for (int i = 0; i < delays.length; i++) {
                assertThat(firedAt.get(i)).as("task with delay %d", delays[i]).isEqualTo(delays[i]);
            }
            assertThat(timer.size()).isZero();
        }
    }

    @Test
    void cancelledTasksNeverRun() throws Exception {
        ManualTime time = new ManualTime();
        List<Integer> fired = Collections.synchronizedList(new ArrayList<>());
        try (SystemTimer timer = new SystemTimer(1, 8, time, Runnable::run)) {
            List<Recorder> tasks = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Recorder r = new Recorder(i, 10 + i * 3L, fired);
                tasks.add(r);
                timer.add(r);
            }
            for (int i = 0; i < 100; i += 2) {
                tasks.get(i).cancel();
            }
            assertThat(timer.size()).isEqualTo(50);
            advance(time, timer, 400);
            assertThat(fired).hasSize(50).allMatch(id -> id % 2 == 1);
        }
    }

    @Test
    void zeroDelayRunsImmediately() {
        ManualTime time = new ManualTime();
        List<Integer> fired = new ArrayList<>();
        try (SystemTimer timer = new SystemTimer(1, 8, time, Runnable::run)) {
            timer.add(new Recorder(1, 0, fired));
            assertThat(fired).containsExactly(1);
        }
    }
}
