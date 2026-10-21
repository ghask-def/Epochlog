package io.epochlog.stress;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

import io.epochlog.concurrent.MpscArrayQueue;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.III_Result;

/**
 * A full queue of capacity 2: the consumer frees one slot while two producers compete for it.
 * At most one producer may succeed, and only if the consumer actually freed the slot. Checks
 * that the producer limit cache never lets a producer overwrite an unconsumed slot.
 * r1/r2 are 1 if producer 1/2 succeeded, r3 is the sum of everything ever consumed.
 */
@JCStressTest
@Outcome(id = {"0, 0, 30", "1, 0, 130", "0, 1, 230"}, expect = ACCEPTABLE, desc = "consistent")
@Outcome(expect = FORBIDDEN, desc = "overwrite or loss")
@State
public class MpscQueueWrapTest {
    private final MpscArrayQueue<Integer> queue = new MpscArrayQueue<>(2);
    private int polled;

    public MpscQueueWrapTest() {
        queue.offer(10);
        queue.offer(20);
    }

    @Actor
    public void producer1(III_Result r) {
        r.r1 = queue.offer(100) ? 1 : 0;
    }

    @Actor
    public void producer2(III_Result r) {
        r.r2 = queue.offer(200) ? 1 : 0;
    }

    @Actor
    public void consumer() {
        Integer v = queue.poll();
        polled = v == null ? 0 : v;
    }

    @Arbiter
    public void drain(III_Result r) {
        int sum = polled;
        Integer v;
        while ((v = queue.poll()) != null) {
            sum += v;
        }
        r.r3 = sum;
    }
}
