package io.epochlog.stress;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

import io.epochlog.concurrent.MpscArrayQueue;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

/**
 * Two producers race a consumer. Whatever the consumer grabs mid-race plus whatever is left
 * afterwards must be exactly {1, 2}: nothing lost, nothing duplicated, no torn slot.
 * r1 is the value polled during the race (0 for none), r2 the sum drained by the arbiter.
 */
@JCStressTest
@Outcome(id = {"0, 3", "1, 2", "2, 1"}, expect = ACCEPTABLE, desc = "every element seen exactly once")
@Outcome(expect = FORBIDDEN, desc = "lost, duplicated or corrupted element")
@State
public class MpscQueueRaceTest {
    private final MpscArrayQueue<Integer> queue = new MpscArrayQueue<>(4);

    @Actor
    public void producer1() {
        queue.offer(1);
    }

    @Actor
    public void producer2() {
        queue.offer(2);
    }

    @Actor
    public void consumer(II_Result r) {
        Integer v = queue.poll();
        r.r1 = v == null ? 0 : v;
    }

    @Arbiter
    public void drain(II_Result r) {
        int sum = 0;
        Integer v;
        while ((v = queue.poll()) != null) {
            sum += v;
        }
        r.r2 = sum;
    }
}
