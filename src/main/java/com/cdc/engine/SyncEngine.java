package com.cdc.engine;

import com.cdc.config.CatalogConfig;
import com.cdc.config.DestinationConfig;
import com.cdc.config.SourceConfig;
import com.cdc.config.StreamEntry;
import com.cdc.destination.iceberg.IcebergWriter;
import com.cdc.destination.parquet.CdcParquetWriter;
import com.cdc.protocol.Driver;
import com.cdc.protocol.SyncMode;
import com.cdc.protocol.Writer;
import com.cdc.protocol.schema.CdcSchema;
import com.cdc.source.postgres.PostgresConnector;
import com.cdc.source.postgres.PostgresDriver;
import com.cdc.state.StateManager;
import com.cdc.state.SyncState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class SyncEngine {
    private static final Logger log = LoggerFactory.getLogger(SyncEngine.class);

    private final SourceConfig sourceConfig;
    private final DestinationConfig destinationConfig;
    private final CatalogConfig catalogConfig;
    private final StateManager stateManager;
    private final boolean fullRefresh;

    public SyncEngine(SourceConfig sourceConfig, DestinationConfig destinationConfig,
                      CatalogConfig catalogConfig, Path statePath, boolean fullRefresh) {
        this.sourceConfig = sourceConfig;
        this.destinationConfig = destinationConfig;
        this.catalogConfig = catalogConfig;
        this.stateManager = new StateManager(statePath);
        this.fullRefresh = fullRefresh;
    }

    public void run() {
        log.info("Starting sync engine");
        long startTime = System.currentTimeMillis();

        try {
            // Load state
            SyncState state = stateManager.load();
            if (fullRefresh) {
                log.info("Full refresh requested, resetting state");
                state = new SyncState();
            }

            // Discover schemas
            PostgresConnector connector = new PostgresConnector();
            List<CdcSchema> allSchemas = connector.discover(sourceConfig);
            log.info("Discovered {} tables", allSchemas.size());

            // Filter to configured streams
            Set<String> configuredTables = catalogConfig.getStreams().stream()
                .map(StreamEntry::getTable)
                .collect(Collectors.toSet());

            List<CdcSchema> targetSchemas = allSchemas.stream()
                .filter(s -> configuredTables.contains(s.fullTableName()))
                .toList();

            if (targetSchemas.isEmpty()) {
                log.warn("No matching tables found for configured streams. Available: {}",
                    allSchemas.stream().map(CdcSchema::fullTableName).toList());
                return;
            }

            log.info("Target tables: {}", targetSchemas.stream().map(CdcSchema::fullTableName).toList());

            // Create map of stream entries for sync mode lookup
            Map<String, StreamEntry> streamEntryMap = catalogConfig.getStreams().stream()
                .collect(Collectors.toMap(StreamEntry::getTable, e -> e));

            // Initialize driver
            PostgresDriver driver = new PostgresDriver();
            driver.init(sourceConfig, targetSchemas);

            try {
                // Phase 1: Full Load for tables that need it
                List<CdcSchema> cdcSchemas = new ArrayList<>();

                for (CdcSchema schema : targetSchemas) {
                    StreamEntry entry = streamEntryMap.get(schema.fullTableName());
                    SyncMode mode = entry != null ? entry.parseSyncMode() : SyncMode.FULL_LOAD_CDC;
                    SyncState.StreamState streamState = state.getOrCreateStream(schema.fullTableName());

                    boolean needsFullLoad = (mode == SyncMode.FULL_LOAD || mode == SyncMode.FULL_LOAD_CDC)
                        && !streamState.isFullLoadCompleted();

                    if (needsFullLoad) {
                        log.info("Running full load for {}", schema.fullTableName());
                        Writer writer = createWriter();
                        try {
                            writer.open(schema, destinationConfig);
                            driver.fullLoad(schema, writer, state);
                        } finally {
                            writer.close();
                        }
                        stateManager.save();
                    }

                    if (mode == SyncMode.CDC || mode == SyncMode.FULL_LOAD_CDC) {
                        cdcSchemas.add(schema);
                    }
                }

                // Phase 2: CDC streaming for all CDC-enabled tables
                if (!cdcSchemas.isEmpty()) {
                    log.info("Starting CDC for {} tables", cdcSchemas.size());

                    // Open a writer per table (or a shared writer)
                    Writer writer = createWriter();
                    try {
                        // Open writer for the first schema (will handle multi-table in records)
                        writer.open(cdcSchemas.get(0), destinationConfig);
                        driver.cdcStream(cdcSchemas, writer, state, () -> {});
                    } finally {
                        writer.close();
                    }
                }

                stateManager.save();
            } finally {
                driver.close();
            }

            long duration = System.currentTimeMillis() - startTime;
            log.info("Sync completed in {}ms", duration);
        } catch (Exception e) {
            log.error("Sync failed", e);
            try {
                stateManager.save();
            } catch (Exception saveErr) {
                log.error("Failed to save state after error", saveErr);
            }
            throw new RuntimeException("Sync failed", e);
        }
    }

    private Writer createWriter() {
        if (destinationConfig.isIceberg()) {
            return new IcebergWriter();
        }
        return new CdcParquetWriter();
    }
}
