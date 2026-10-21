package io.epochlog.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

class ConfigTest {

    @Test
    void parsesSizesAndDurations() {
        Config c = new Config(Map.of("seg", "64m", "retention", "7d", "wait", "2ms", "lag", "10s", "n", "12"));
        assertThat(c.getLong("seg", 0)).isEqualTo(64L << 20);
        assertThat(c.getDurationMs("wait", 0)).isEqualTo(2);
        assertThat(c.getDurationMs("lag", 0)).isEqualTo(10_000);
        assertThat(c.getInt("n", 0)).isEqualTo(12);
        assertThat(c.getInt("missing", 5)).isEqualTo(5);
        assertThatThrownBy(() -> c.getDurationMs("retention", 0)).isInstanceOf(EpochlogException.class);
    }

    @Test
    void topicPartitionParsesNamesWithDashes() {
        assertThat(TopicPartition.parse("my-topic-12")).isEqualTo(new TopicPartition("my-topic", 12));
        assertThat(new TopicPartition("a-b", 3).toString()).isEqualTo("a-b-3");
    }

    @Test
    void hybridTimeOrderingMatchesLongOrdering() {
        long a = HybridTime.pack(1000, 65535);
        long b = HybridTime.pack(1001, 0);
        assertThat(a).isLessThan(b);
        assertThat(HybridTime.physicalMillis(a)).isEqualTo(1000);
        assertThat(HybridTime.logical(a)).isEqualTo(65535);
    }
}
