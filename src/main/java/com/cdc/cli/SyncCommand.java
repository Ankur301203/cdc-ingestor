package com.cdc.cli;

import com.cdc.config.CatalogConfig;
import com.cdc.config.ConfigLoader;
import com.cdc.config.DestinationConfig;
import com.cdc.config.SourceConfig;
import com.cdc.engine.SyncEngine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;

@Command(name = "sync", description = "Run data synchronization (Full Load and/or CDC)")
public class SyncCommand implements Runnable {

    @Option(names = "--source", required = true, description = "Path to source config YAML")
    private Path sourcePath;

    @Option(names = "--destination", required = true, description = "Path to destination config YAML")
    private Path destinationPath;

    @Option(names = "--catalog", required = true, description = "Path to catalog config YAML")
    private Path catalogPath;

    @Option(names = "--state", required = true, description = "Path to state JSON file")
    private Path statePath;

    @Option(names = "--full-refresh", description = "Force full refresh (re-snapshot all tables)")
    private boolean fullRefresh;

    @Override
    public void run() {
        try {
            SourceConfig sourceConfig = ConfigLoader.loadSource(sourcePath);
            DestinationConfig destConfig = ConfigLoader.loadDestination(destinationPath);
            CatalogConfig catalogConfig = ConfigLoader.loadCatalog(catalogPath);

            SyncEngine engine = new SyncEngine(
                sourceConfig, destConfig, catalogConfig, statePath, fullRefresh);
            engine.run();
        } catch (Exception e) {
            System.err.println("Sync failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
