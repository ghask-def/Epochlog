package io.epochlog.stress;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

import java.nio.ByteBuffer;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.JJ_Result;

/**
 * The publication pattern used by {@code Log} and the mmap'd indexes: the writer stores data
 * into an off-heap buffer (as a MappedByteBuffer index entry is written) and then writes the
 * volatile end offset. A reader that observes the new end offset must observe the data.
 */
@JCStressTest
@Outcome(id = "0, -1", expect = ACCEPTABLE, desc = "reader ran before publish")
@Outcome(id = "1, 42", expect = ACCEPTABLE, desc = "reader saw publish and data")
@Outcome(id = "1, 0", expect = FORBIDDEN, desc = "saw end offset but not the bytes behind it")
@Outcome(expect = FORBIDDEN, desc = "anything else")
@State
public class LogEndOffsetVisibilityTest {
    private final ByteBuffer data = ByteBuffer.allocateDirect(64);
    private volatile long logEndOffset;

    @Actor
    public void writer() {
        data.putLong(8, 42L);
        logEndOffset = 1;
    }

    @Actor
    public void reader(JJ_Result r) {
        long end = logEndOffset;
        r.r1 = end;
        r.r2 = end == 1 ? data.getLong(8) : -1;
    }
}
