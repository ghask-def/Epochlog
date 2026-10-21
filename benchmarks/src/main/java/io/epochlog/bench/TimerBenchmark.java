package io.epochlog.bench;

import io.epochlog.common.Time;
import io.epochlog.concurrent.SystemTimer;
import io.epochlog.concurrent.TimerTask;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Add-then-cancel, the common path for acks=all requests that complete before their timeout,
 * measured with 100k other tasks already pending to show the cost doesn't grow with timer size.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 2)
@Fork(1)
@State(Scope.Benchmark)
public class TimerBenchmark {
    SystemTimer timer;

    @Setup
    public void setup() {
        timer = SystemTimer.create(Time.SYSTEM);
        timer.startReaper("bench-reaper");
        // Background load: 100k pending tasks spread over the next minute.
        for (int i = 0; i < 100_000; i++) {
            timer.add(new Noop(30_000 + i % 30_000));
        }
    }

    @TearDown
    public void tearDown() {
        timer.close();
    }

    @Benchmark
    public void addAndCancel() {
        Noop t = new Noop(30_000);
        timer.add(t);
        t.cancel();
    }

    static final class Noop extends TimerTask {
        Noop(long delay) {
            super(delay);
        }

        @Override
        public void run() {}
    }
}
