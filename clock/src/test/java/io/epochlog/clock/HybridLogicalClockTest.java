package io.epochlog.clock;

import static io.epochlog.common.HybridTime.logical;
import static io.epochlog.common.HybridTime.pack;
import static io.epochlog.common.HybridTime.physicalMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class HybridLogicalClockTest {

    private final AtomicLong wall = new AtomicLong(10_000);
    private final HybridLogicalClock clock = new HybridLogicalClock(wall::get, 500);

    @Test
    void tracksPhysicalTimeWhenItMovesForward() {
        assertThat(clock.now()).isEqualTo(pack(10_000, 0));
        wall.set(10_005);
        assertThat(clock.now()).isEqualTo(pack(10_005, 0));
    }

    @Test
    void logicalCounterBreaksTiesWhenPhysicalTimeStalls() {
        long a = clock.now();
        long b = clock.now();
        long c = clock.now();
        assertThat(physicalMillis(c)).isEqualTo(10_000);
        assertThat(List.of(logical(a), logical(b), logical(c))).containsExactly(0, 1, 2);
    }

    @Test
    void neverGoesBackwardsWhenWallClockSteps() {
        wall.set(20_000);
        long before = clock.now();
        wall.set(15_000);
        long after = clock.now();
        assertThat(after).isGreaterThan(before);
        assertThat(physicalMillis(after)).isEqualTo(20_000);
    }

    @Test
    void updateMovesPastRemoteTimestamps() {
        long remote = pack(10_300, 7);
        long merged = clock.update(remote);
        assertThat(merged).isEqualTo(pack(10_300, 8));
        assertThat(clock.now()).isGreaterThan(merged);
    }

    @Test
    void updateWithOlderRemoteJustTicks() {
        wall.set(50_000);
        long local = clock.now();
        assertThat(clock.update(pack(40_000, 3))).isEqualTo(local + 1);
    }

    @Test
    void rejectsTimestampsBeyondMaxDrift() {
        long before = clock.now();
        assertThatThrownBy(() -> clock.update(pack(10_501, 0))).isInstanceOf(ClockDriftException.class);
        assertThat(clock.driftRejections()).isEqualTo(1);
        assertThat(clock.current()).isEqualTo(before);
        assertThat(clock.observe(pack(99_999, 0))).isEqualTo(before + 1);
        assertThat(clock.driftRejections()).isEqualTo(2);
        clock.update(pack(10_500, 0));
    }

    @Test
    void reserveHandsOutContiguousRanges() {
        long a = clock.reserve(10);
        long b = clock.reserve(5);
        assertThat(b).isEqualTo(a + 10);
        assertThat(clock.current()).isEqualTo(b + 4);
    }

    @Test
    void logicalOverflowCarriesIntoPhysical() {
        long base = clock.reserve(70_000);
        long next = clock.now();
        assertThat(next).isEqualTo(base + 70_000);
        assertThat(physicalMillis(next)).isEqualTo(10_001);
    }

    @Test
    void concurrentCallersGetUniqueIncreasingTimestamps() throws Exception {
        int threads = 8;
        int each = 50_000;
        List<long[]> results = new ArrayList<>();
        List<Thread> ts = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            long[] out = new long[each];
            results.add(out);
            Thread th = new Thread(() -> {
                for (int i = 0; i < each; i++) {
                    out[i] = (i % 3 == 0) ? clock.update(pack(wall.get() - 1, i)) : clock.now();
                    if (i % 1000 == 0) {
                        wall.incrementAndGet();
                    }
                }
            });
            ts.add(th);
            th.start();
        }
        for (Thread th : ts) {
            th.join();
        }
        Set<Long> all = new HashSet<>();
        for (long[] out : results) {
            for (int i = 1; i < out.length; i++) {
                assertThat(out[i]).isGreaterThan(out[i - 1]);
            }
            for (long v : out) {
                all.add(v);
            }
        }
        assertThat(all).hasSize(threads * each);
    }

    @Test
    void skewedNodesStayCausallyOrdered() {
        AtomicLong fastWall = new AtomicLong(10_300);
        HybridLogicalClock fast = new HybridLogicalClock(fastWall::get, 500);
        AtomicLong slowWall = new AtomicLong(10_000);
        HybridLogicalClock slow = new HybridLogicalClock(slowWall::get, 500);

        long sent = fast.now();
        long received = slow.update(sent);
        long reply = slow.now();
        assertThat(received).isGreaterThan(sent);
        assertThat(reply).isGreaterThan(received);
        assertThat(fast.update(reply)).isGreaterThan(reply);
    }
}
