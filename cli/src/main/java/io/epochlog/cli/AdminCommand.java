package io.epochlog.cli;

import io.epochlog.client.Admin;
import io.epochlog.protocol.message.BrokerInfo;
import io.epochlog.protocol.message.MetadataResponse;
import io.epochlog.protocol.message.PartitionState;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Topic and cluster administration.
 *
 * <p>Thread-safety: not applicable (entry point).
 */
@Command(name = "admin", mixinStandardHelpOptions = true, description = "Create and inspect topics.",
        subcommands = {AdminCommand.CreateTopic.class, AdminCommand.Describe.class})
public final class AdminCommand implements Runnable {

    @Override
    public void run() {
        new CommandLine(this).usage(System.out);
    }

    /**
     * {@code admin create-topic}.
     *
     * <p>Thread-safety: not applicable.
     */
    @Command(name = "create-topic", mixinStandardHelpOptions = true, description = "Create a topic.")
    static final class CreateTopic implements Callable<Integer> {
        @Option(names = {"-b", "--bootstrap"}, required = true)
        String bootstrap;

        @Parameters(index = "0", description = "Topic name")
        String name;

        @Option(names = {"-p", "--partitions"}, defaultValue = "1")
        int partitions;

        @Option(names = {"-r", "--replication-factor"}, defaultValue = "1")
        int replicationFactor;

        @Option(names = {"-c", "--config"}, description = "key=value topic config, e.g. cleanup.policy=compact")
        Map<String, String> configs = new LinkedHashMap<>();

        @Override
        public Integer call() throws Exception {
            try (Admin admin = new Admin(bootstrap)) {
                admin.createTopic(name, partitions, replicationFactor, configs);
            }
            System.out.printf("created %s (%d partitions, replication factor %d)%n", name, partitions, replicationFactor);
            return 0;
        }
    }

    /**
     * {@code admin describe}.
     *
     * <p>Thread-safety: not applicable.
     */
    @Command(name = "describe", mixinStandardHelpOptions = true, description = "Show brokers and partition leaders.")
    static final class Describe implements Callable<Integer> {
        @Option(names = {"-b", "--bootstrap"}, required = true)
        String bootstrap;

        @Parameters(arity = "0..*", description = "Topics (default: all)")
        List<String> topics = List.of();

        @Override
        public Integer call() throws Exception {
            MetadataResponse md;
            try (Admin admin = new Admin(bootstrap)) {
                md = admin.describe(topics);
            }
            System.out.println("brokers:");
            for (BrokerInfo b : md.brokers()) {
                System.out.printf("  %d  %s:%d%n", b.id(), b.host(), b.port());
            }
            for (MetadataResponse.TopicMetadata t : md.topics()) {
                System.out.printf("%s%s%n", t.name(), t.error().name().equals("NONE") ? "" : "  (" + t.error() + ")");
                for (PartitionState p : t.partitions()) {
                    System.out.printf("  partition %-3d leader %-3d epoch %-4d replicas %-12s isr %s%n", p.partition(),
                            p.leader(), p.leaderEpoch(), p.replicas(), p.isr());
                }
            }
            return 0;
        }
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new AdminCommand()).execute(args));
    }
}
