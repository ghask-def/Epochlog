package io.epochlog.stress;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

import io.epochlog.clock.HybridLogicalClock;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.ZZ_Result;

/**
 * Concurrent now() and update() on a clock whose physical time never moves: every issued
 * timestamp must be distinct, and the clock must end up past both. r1: distinct, r2: current
 * is the max of both.
 */
@JCStressTest
@Outcome(id = "true, true", expect = ACCEPTABLE, desc = "unique and monotonic")
@Outcome(expect = FORBIDDEN, desc = "duplicate timestamp or clock went backwards")
@State
public class HlcUniquenessTest {
    private final HybridLogicalClock clock = new HybridLogicalClock(() -> 1_000L, 500);
    private long a;
    private long b;

    @Actor
    public void local() {
        a = clock.now();
    }

    @Actor
    public void receive() {
        b = clock.update(0x3E8_0005L);
    }

    @Arbiter
    public void check(ZZ_Result r) {
        r.r1 = a != b;
        r.r2 = clock.current() == Math.max(a, b);
    }
}
