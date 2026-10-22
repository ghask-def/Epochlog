package io.epochlog.cli;

import io.epochlog.client.Consumer;
import io.epochlog.client.ConsumerRecord;
import io.epochlog.common.HybridTime;
import io.epochlog.common.TopicPartition;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Prints records from a topic to stdout.
 *
 * <p>Thread-safety: not applicable (entry point).
 */
@Command(name = "consume", mixinStandardHelpOptions = true, description = "Print records from a topic.")
public final class ConsumeCommand implements Callable<Integer> {

    @Option(names = {"-b", "--bootstrap"}, required = true)
    String bootstrap;

    @Option(names = {"-t", "--topic"}, required = true)
    String topic;

    @Option(names = {"-p", "--partition"}, description = "Only this partition (default: all)")
    Integer partition;

    @Option(names = "--from-beginning", description = "Start at the earliest offset instead of the end")
    boolean fromBeginning;

    @Option(names = "--offset", description = "Start at this offset (requires --partition)")
    Long offset;

    @Option(names = "--from-time", description = "Start at the first record at or after this ISO-8601 instant")
    String fromTime;

    @Option(names = {"-g", "--group"}, description = "Resume from and commit to this consumer group")
    String group;

    @Option(names = {"-n", "--max-messages"}, description = "Exit after this many records")
    Long maxMessages;

    @Option(names = "--timeout-ms", description = "Exit if nothing arrives for this long")
    Long idleTimeoutMs;

    @Option(names = "--print-metadata", description = "Prefix each value with partition, offset and HLC timestamp")
    boolean printMetadata;

    @Override
    public Integer call() throws Exception {
        if (offset != null && partition == null) {
            throw new CommandLine.ParameterException(new CommandLine(this), "--offset needs --partition");
        }
        Map<String, String> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrap);
        props.put("auto.offset.reset", fromBeginning ? "earliest" : "latest");
        if (group != null) {
            props.put("group.id", group);
        }
        long seen = 0;
        long lastActivity = System.currentTimeMillis();
        try (Consumer consumer = new Consumer(props)) {
            List<TopicPartition> tps = partition == null ? consumer.assignTopic(topic)
                    : List.of(new TopicPartition(topic, partition));
            consumer.assign(tps);
            if (offset != null) {
                consumer.seek(tps.get(0), offset);
            } else if (fromTime != null) {
                long hlc = HybridTime.fromMillis(Instant.parse(fromTime).toEpochMilli());
                for (TopicPartition tp : tps) {
                    consumer.seekToTimestamp(tp, hlc);
                }
            }
            while (maxMessages == null || seen < maxMessages) {
                List<ConsumerRecord> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord r : records) {
                    if (printMetadata) {
                        System.out.printf("%d\t%d\t%s\t%s%n", r.partition(), r.offset(), HybridTime.format(r.timestamp()),
                                r.valueString());
                    } else {
                        System.out.println(r.valueString());
                    }
                    if (maxMessages != null && ++seen >= maxMessages) {
                        break;
                    } else if (maxMessages == null) {
                        seen++;
                    }
                }
                if (!records.isEmpty()) {
                    lastActivity = System.currentTimeMillis();
                    if (group != null) {
                        consumer.commitSync();
                    }
                } else if (idleTimeoutMs != null && System.currentTimeMillis() - lastActivity > idleTimeoutMs) {
                    break;
                }
            }
        }
        System.out.flush();
        return 0;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new ConsumeCommand()).execute(args));
    }
}
