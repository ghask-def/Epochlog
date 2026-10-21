package io.epochlog.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LeaderEpochCacheTest {
    @TempDir
    Path dir;

    @Test
    void endOffsetLookups() throws Exception {
        LeaderEpochCache cache = new LeaderEpochCache(dir.resolve("cp"));
        cache.assign(1, 0);
        cache.assign(3, 50);
        cache.assign(4, 80);
        cache.assign(2, 60);

        assertThat(cache.endOffsetFor(4, 100)).isEqualTo(new OffsetAndEpoch(100, 4));
        assertThat(cache.endOffsetFor(3, 100)).isEqualTo(new OffsetAndEpoch(80, 3));
        // Epoch 2 never had a leader here: report the end of the closest earlier epoch.
        assertThat(cache.endOffsetFor(2, 100)).isEqualTo(new OffsetAndEpoch(50, 1));
        assertThat(cache.endOffsetFor(0, 100)).isEqualTo(new OffsetAndEpoch(0, 0));
        assertThat(cache.endOffsetFor(7, 100)).isEqualTo(OffsetAndEpoch.UNDEFINED);
    }

    @Test
    void persistsAndTruncates() throws Exception {
        Path file = dir.resolve("cp");
        LeaderEpochCache cache = new LeaderEpochCache(file);
        cache.assign(0, 0);
        cache.assign(1, 10);
        cache.assign(2, 20);
        cache.truncateFromEnd(15);
        cache.truncateFromStart(5);

        LeaderEpochCache reloaded = new LeaderEpochCache(file);
        assertThat(reloaded.entries()).containsExactly(
                new LeaderEpochCache.EpochEntry(0, 5),
                new LeaderEpochCache.EpochEntry(1, 10));
        assertThat(reloaded.latestEpoch()).isEqualTo(1);
    }
}
