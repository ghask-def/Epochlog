package io.epochlog.bench;

import io.epochlog.concurrent.MpscArrayQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/** Three producers and one consumer: the hand-off from request handlers to a partition appender. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 2)
@Fork(1)
public class QueueBenchmark {
    private static final Integer ITEM = 1;

    @State(Scope.Group)
    public static class Mpsc {
        final MpscArrayQueue<Integer> q = new MpscArrayQueue<>(8192);
    }

    @State(Scope.Group)
    public static class Blocking {
        final ArrayBlockingQueue<Integer> q = new ArrayBlockingQueue<>(8192);
    }

    @Benchmark
    @Group("mpsc")
    @GroupThreads(3)
    public boolean mpscOffer(Mpsc s) {
        return s.q.offer(ITEM);
    }

    @Benchmark
    @Group("mpsc")
    @GroupThreads(1)
    public Integer mpscPoll(Mpsc s) {
        return s.q.poll();
    }

    @Benchmark
    @Group("abq")
    @GroupThreads(3)
    public boolean abqOffer(Blocking s) {
        return s.q.offer(ITEM);
    }

    @Benchmark
    @Group("abq")
    @GroupThreads(1)
    public Integer abqPoll(Blocking s) {
        return s.q.poll();
    }
}
