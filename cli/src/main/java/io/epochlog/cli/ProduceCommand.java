package io.epochlog.cli;

import io.epochlog.client.Producer;
import io.epochlog.client.ProducerRecord;
import io.epochlog.client.RecordMetadata;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Reads lines from stdin and produces one record per line. With {@code --key-separator}, the text
 * before the separator becomes the key.
 *
 * <p>Thread-safety: not applicable (entry point).
 */
@Command(name = "produce", mixinStandardHelpOptions = true, description = "Produce lines from stdin to a topic.")
public final class ProduceCommand implements Callable<Integer> {

    @Option(names = {"-b", "--bootstrap"}, required = true, description = "host:port[,host:port...]")
    String bootstrap;

    @Option(names = {"-t", "--topic"}, required = true)
    String topic;

    @Option(names = {"-p", "--partition"}, description = "Fixed partition (default: by key, or sticky)")
    Integer partition;

    @Option(names = "--acks", defaultValue = "all", description = "0, 1 or all (default: ${DEFAULT-VALUE})")
    String acks;

    @Option(names = "--key-separator", description = "Split each line into key and value at this string")
    String keySeparator;

    @Option(names = "--linger-ms", defaultValue = "5")
    long lingerMs;

    @Option(names = "--compression", defaultValue = "none", description = "none or gzip")
    String compression;

    @Option(names = {"-v", "--verbose"}, description = "Print the offset of every record")
    boolean verbose;

    @Override
    public Integer call() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("bootstrap.servers", bootstrap);
        props.put("acks", acks);
        props.put("linger.ms", Long.toString(lingerMs));
        props.put("compression.type", compression);
        props.put("enable.idempotence", Boolean.toString(!acks.equals("0")));
        AtomicLong failed = new AtomicLong();
        long sent = 0;
        try (Producer producer = new Producer(props);
                BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = in.readLine()) != null) {
                byte[] key = null;
                String value = line;
                if (keySeparator != null) {
                    int i = line.indexOf(keySeparator);
                    if (i >= 0) {
                        key = line.substring(0, i).getBytes(StandardCharsets.UTF_8);
                        value = line.substring(i + keySeparator.length());
                    }
                }
                CompletableFuture<RecordMetadata> f = producer.send(new ProducerRecord(topic, partition, key,
                        value.getBytes(StandardCharsets.UTF_8), null));
                f.whenComplete((md, err) -> {
                    if (err != null) {
                        failed.incrementAndGet();
                        System.err.println("send failed: " + err.getMessage());
                    } else if (verbose) {
                        System.err.println(md.topicPartition() + "@" + md.offset());
                    }
                });
                sent++;
            }
            producer.flush();
        }
        System.err.printf("sent %d records, %d failed%n", sent, failed.get());
        return failed.get() == 0 ? 0 : 1;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new ProduceCommand()).execute(args));
    }
}
