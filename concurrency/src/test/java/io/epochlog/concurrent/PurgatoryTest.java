package io.epochlog.concurrent;

import static org.assertj.core.api.Assertions.assertThat;

import io.epochlog.common.Time;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class PurgatoryTest {

    /** Completes when the shared counter reaches a target. */
    static final class WaitForCount extends DelayedOperation {
        final AtomicLong counter;
        final long target;
        final CompletableFuture<Boolean> result = new CompletableFuture<>();
        final AtomicInteger completions;

        WaitForCount(AtomicLong counter, long target, long timeoutMs, AtomicInteger completions) {
            super(timeoutMs);
            this.counter = counter;
            this.target = target;
            this.completions = completions;
        }

        @Override
        public boolean tryComplete() {
            return counter.get() >= target && forceComplete();
        }

        @Override
        protected void onComplete() {
            completions.incrementAndGet();
            result.complete(counter.get() >= target);
        }
    }

    @Test
    void completesWhenConditionIsMet() throws Exception {
        AtomicLong counter = new AtomicLong();
        AtomicInteger completions = new AtomicInteger();
        try (DelayedOperationPurgatory<WaitForCount> p = new DelayedOperationPurgatory<>("test", Time.SYSTEM, 100)) {
            WaitForCount op = new WaitForCount(counter, 5, 30_000, completions);
            assertThat(p.tryCompleteElseWatch(op, List.of("k"))).isFalse();
            assertThat(p.watched("k")).isEqualTo(1);
            counter.set(4);
            assertThat(p.checkAndComplete("k")).isZero();
            counter.set(5);
            assertThat(p.checkAndComplete("k")).isEqualTo(1);
            assertThat(op.result.get(1, TimeUnit.SECONDS)).isTrue();
            assertThat(p.watched("k")).isZero();
        }
        assertThat(completions.get()).isEqualTo(1);
    }

    @Test
    void timesOut() throws Exception {
        AtomicLong counter = new AtomicLong();
        AtomicInteger completions = new AtomicInteger();
        try (DelayedOperationPurgatory<WaitForCount> p = new DelayedOperationPurgatory<>("test", Time.SYSTEM, 100)) {
            WaitForCount op = new WaitForCount(counter, 1, 20, completions);
            p.tryCompleteElseWatch(op, List.of("k"));
            assertThat(op.result.get(2, TimeUnit.SECONDS)).isFalse();
            counter.set(1);
            assertThat(p.checkAndComplete("k")).isZero();
        }
        assertThat(completions.get()).isEqualTo(1);
    }

    @Test
    void racingCompletionAndTimeoutCompleteOnce() throws Exception {
        AtomicLong counter = new AtomicLong();
        AtomicInteger completions = new AtomicInteger();
        int ops = 2_000;
        try (DelayedOperationPurgatory<WaitForCount> p = new DelayedOperationPurgatory<>("test", Time.SYSTEM, 100)) {
            WaitForCount[] all = new WaitForCount[ops];
            for (int i = 0; i < ops; i++) {
                all[i] = new WaitForCount(counter, 1, i % 5, completions);
                p.tryCompleteElseWatch(all[i], List.of("k"));
            }
            counter.set(1);
            Thread a = new Thread(() -> p.checkAndComplete("k"));
            Thread b = new Thread(() -> p.checkAndComplete("k"));
            a.start();
            b.start();
            a.join();
            b.join();
            for (WaitForCount op : all) {
                op.result.get(2, TimeUnit.SECONDS);
            }
        }
        assertThat(completions.get()).isEqualTo(ops);
    }
}
