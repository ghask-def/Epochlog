package io.epochlog.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IndexTest {
    @TempDir
    Path dir;

    @Test
    void offsetIndexFindsLargestEntryAtOrBelowTarget() throws Exception {
        OffsetIndex idx = new OffsetIndex(dir.resolve("0.index"), 100, 1024, true);
        for (int i = 0; i < 10; i++) {
            idx.append(100 + i * 10, i * 1000);
        }
        assertThat(idx.lookup(99)).isEqualTo(new OffsetIndex.OffsetPosition(100, 0));
        assertThat(idx.lookup(100)).isEqualTo(new OffsetIndex.OffsetPosition(100, 0));
        assertThat(idx.lookup(155)).isEqualTo(new OffsetIndex.OffsetPosition(150, 5000));
        assertThat(idx.lookup(1_000)).isEqualTo(new OffsetIndex.OffsetPosition(190, 9000));
        assertThatThrownBy(() -> idx.append(150, 1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void truncateAndReopenAfterTrim() throws Exception {
        Path file = dir.resolve("0.index");
        OffsetIndex idx = new OffsetIndex(file, 0, 1024, true);
        for (int i = 0; i < 5; i++) {
            idx.append(i * 2, i * 100);
        }
        idx.truncateTo(5);
        assertThat(idx.entries()).isEqualTo(3);
        assertThat(idx.lastOffset()).isEqualTo(4);
        idx.close();
        assertThat(Files.size(file)).isEqualTo(24);

        OffsetIndex reopened = new OffsetIndex(file, 0, 1024, false);
        assertThat(reopened.entries()).isEqualTo(3);
        assertThat(reopened.lookup(3).position()).isEqualTo(100);
        assertThat(reopened.sanityCheck(1_000)).isTrue();
        assertThat(reopened.sanityCheck(150)).isFalse();
    }

    @Test
    void fillsUp() throws Exception {
        OffsetIndex idx = new OffsetIndex(dir.resolve("0.index"), 0, 64, true);
        for (int i = 0; i < 8; i++) {
            idx.append(i, i);
        }
        assertThat(idx.isFull()).isTrue();
        assertThatThrownBy(() -> idx.append(9, 9)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void timeIndexSkipsNonIncreasingTimestamps() throws Exception {
        TimeIndex idx = new TimeIndex(dir.resolve("0.timeindex"), 0, 1200, true);
        idx.maybeAppend(1000, 0);
        idx.maybeAppend(900, 5);
        idx.maybeAppend(2000, 10);
        idx.maybeAppend(3000, 20);
        assertThat(idx.entries()).isEqualTo(3);
        assertThat(idx.lookup(500)).isEqualTo(new TimeIndex.TimestampOffset(-1, 0));
        assertThat(idx.lookup(2500)).isEqualTo(new TimeIndex.TimestampOffset(2000, 10));
        idx.truncateTo(15);
        assertThat(idx.lastTimestamp()).isEqualTo(2000);
    }
}
