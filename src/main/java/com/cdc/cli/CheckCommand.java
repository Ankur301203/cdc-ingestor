package com.cdc.cli;

import com.cdc.config.ConfigLoader;
import com.cdc.config.SourceConfig;
import com.cdc.protocol.CheckResult;
import com.cdc.protocol.Connector;
import com.cdc.source.SourceConnectorFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;

@Command(name = "check", description = "Validate source connection")
public class CheckCommand implements Runnable {

    @Option(names = "--source", required = true, description = "Path to source config YAML")
    private Path sourcePath;

    @Override
    public void run() {
        try {
            SourceConfig config = ConfigLoader.loadSource(sourcePath);
            Connector connector = SourceConnectorFactory.forConfig(config);
            CheckResult result = connector.check(config);

            if (result.success()) {
                System.out.println("OK: " + result.message());
            } else {
                System.err.println("FAILED: " + result.message());
                System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }
}
