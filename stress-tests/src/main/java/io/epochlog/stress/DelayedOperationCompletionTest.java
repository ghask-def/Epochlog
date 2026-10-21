package io.epochlog.stress;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

import io.epochlog.concurrent.DelayedOperation;
import java.util.concurrent.atomic.AtomicInteger;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

/**
 * A request in purgatory can be completed by a high watermark update and by its timeout at the
 * same moment. onComplete must run exactly once. r1 counts completions, r2 counts expirations.
 */
@JCStressTest
@Outcome(id = "1, 0", expect = ACCEPTABLE, desc = "completed by the condition")
@Outcome(id = "1, 1", expect = ACCEPTABLE, desc = "completed by the timeout")
@Outcome(expect = FORBIDDEN, desc = "completed twice or not at all")
@State
public class DelayedOperationCompletionTest {
    private final AtomicInteger completions = new AtomicInteger();
    private final AtomicInteger expirations = new AtomicInteger();

    private final DelayedOperation op = new DelayedOperation(1000) {
        @Override
        public boolean tryComplete() {
            return forceComplete();
        }

        @Override
        protected void onComplete() {
            completions.incrementAndGet();
        }

        @Override
        protected void onExpiration() {
            expirations.incrementAndGet();
        }
    };

    @Actor
    public void hwAdvanced() {
        op.tryComplete();
    }

    @Actor
    public void timerFired() {
        op.run();
    }

    @Arbiter
    public void check(II_Result r) {
        r.r1 = completions.get();
        r.r2 = expirations.get();
    }
}
