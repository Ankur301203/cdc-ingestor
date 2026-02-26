package com.cdc;

import com.cdc.cli.CheckCommand;
import com.cdc.cli.DiscoverCommand;
import com.cdc.cli.ProduceKafkaCommand;
import com.cdc.cli.SpecCommand;
import com.cdc.cli.SyncCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
    name = "cdc-ingestor",
    mixinStandardHelpOptions = true,
    version = "1.0.0",
    description = "CDC Ingestor - Change Data Capture from PostgreSQL to Parquet/Iceberg",
    subcommands = {
        SpecCommand.class,
        CheckCommand.class,
        DiscoverCommand.class,
        SyncCommand.class,
        ProduceKafkaCommand.class
    }
)
public class Main implements Runnable {

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }
}
