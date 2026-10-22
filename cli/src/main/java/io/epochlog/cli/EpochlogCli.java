package io.epochlog.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Umbrella command: {@code epochlog produce|consume|admin ...}.
 *
 * <p>Thread-safety: not applicable (entry point).
 */
@Command(name = "epochlog", mixinStandardHelpOptions = true, version = "epochlog 0.1.0",
        subcommands = {ProduceCommand.class, ConsumeCommand.class, AdminCommand.class})
public final class EpochlogCli implements Runnable {

    @Override
    public void run() {
        new CommandLine(this).usage(System.out);
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new EpochlogCli()).execute(args));
    }
}
