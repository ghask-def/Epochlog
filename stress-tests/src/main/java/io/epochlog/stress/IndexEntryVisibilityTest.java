package io.epochlog.stress;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

import io.epochlog.storage.OffsetIndex;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.JI_Result;

/**
 * The real {@link OffsetIndex}: a lookup racing an append returns either the old last entry or
 * the new one, never a half-written entry (new offset with stale position).
 */
@JCStressTest
@Outcome(id = "10, 100", expect = ACCEPTABLE, desc = "lookup before append")
@Outcome(id = "20, 200", expect = ACCEPTABLE, desc = "lookup after append")
@Outcome(expect = FORBIDDEN, desc = "torn entry")
@State
public class IndexEntryVisibilityTest {
    private final OffsetIndex index;
    private final Path file;

    public IndexEntryVisibilityTest() {
        try {
            file = Files.createTempFile("jcstress", ".index");
            Files.delete(file);
            index = new OffsetIndex(file, 0, 64, true);
            index.append(10, 100);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Actor
    public void writer() {
        index.append(20, 200);
    }

    @Actor
    public void reader(JI_Result r) {
        OffsetIndex.OffsetPosition p = index.lookup(25);
        r.r1 = p.offset();
        r.r2 = p.position();
    }

    @Arbiter
    public void cleanup(JI_Result r) {
        try {
            Files.deleteIfExists(file);
        } catch (IOException ignored) {
        }
    }
}
