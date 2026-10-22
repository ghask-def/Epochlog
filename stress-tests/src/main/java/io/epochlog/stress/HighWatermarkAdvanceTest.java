package io.epochlog.stress;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

import io.epochlog.replication.HighWatermark;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.JJJ_Result;

/**
 * Two threads compute different high watermark candidates and race to apply them (a follower
 * fetch and a group commit completing at once), while a reader samples. The final value must be
 * the larger candidate, and the reader must never see anything but a value some writer set.
 */
@JCStressTest
@Outcome(id = {"10, 20, 30", "20, 20, 30", "30, 20, 30"}, expect = ACCEPTABLE, desc = "monotonic, max wins")
@Outcome(expect = FORBIDDEN, desc = "watermark went backwards or lost the max")
@State
public class HighWatermarkAdvanceTest {
    private final HighWatermark hw = new HighWatermark(10);

    @Actor
    public void followerFetch() {
        hw.advanceTo(30);
    }

    @Actor
    public void groupCommit() {
        hw.advanceTo(20);
    }

    @Actor
    public void reader(JJJ_Result r) {
        r.r1 = hw.get();
    }

    @Arbiter
    public void check(JJJ_Result r) {
        r.r2 = 20;
        r.r3 = hw.get();
    }
}
