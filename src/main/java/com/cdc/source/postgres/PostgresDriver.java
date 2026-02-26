package com.cdc.source.postgres;

import com.cdc.config.SourceConfig;
import com.cdc.protocol.Driver;
import com.cdc.protocol.Record;
import com.cdc.protocol.Writer;
import com.cdc.protocol.schema.CdcSchema;
import com.cdc.state.SyncState;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class PostgresDriver implements Driver {
    private static final Logger log = LoggerFactory.getLogger(PostgresDriver.class);
    private static final int FULL_LOAD_THREADS = 4;

    private SourceConfig config;
    private List<CdcSchema> schemas;
    private Connection managementConnection;

    @Override
    public void init(SourceConfig config, List<CdcSchema> schemas) {
        this.config = config;
        this.schemas = schemas;
        try {
            Properties props = new Properties();
            props.setProperty("user", config.getUsername());
            props.setProperty("password", config.getPassword());
            this.managementConnection = DriverManager.getConnection(config.jdbcUrl(), props);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize PostgresDriver", e);
        }
    }

    @Override
    public void fullLoad(CdcSchema schema, Writer writer, SyncState state) {
        log.info("Starting full load for {}", schema.fullTableName());
        SyncState.StreamState streamState = state.getOrCreateStream(schema.fullTableName());

        try {
            CtidChunkSplitter splitter = new CtidChunkSplitter();
            List<CtidChunkSplitter.CtidRange> chunks = splitter.split(
                managementConnection, schema.schemaName(), schema.tableName());

            streamState.setFullLoadChunksTotal(chunks.size());
            int startChunk = streamState.getFullLoadChunksDone();

            if (startChunk > 0) {
                log.info("Resuming full load from chunk {}/{}", startChunk, chunks.size());
            }

            Properties connProps = new Properties();
            connProps.setProperty("user", config.getUsername());
            connProps.setProperty("password", config.getPassword());

            ExecutorService executor = Executors.newFixedThreadPool(FULL_LOAD_THREADS);
            try {
                List<Future<List<Record>>> futures = new ArrayList<>();

                for (int i = startChunk; i < chunks.size(); i++) {
                    CtidChunkReader reader = new CtidChunkReader(
                        config.jdbcUrl(), connProps, schema, chunks.get(i), i);
                    futures.add(executor.submit(reader));
                }

                long totalRecords = 0;
                for (int i = 0; i < futures.size(); i++) {
                    List<Record> records = futures.get(i).get();
                    for (Record record : records) {
                        writer.writeRecord(record);
                    }
                    totalRecords += records.size();

                    streamState.setFullLoadChunksDone(startChunk + i + 1);
                    streamState.setLastSyncTime(Instant.now());

                    if ((i + 1) % 10 == 0) {
                        log.info("Full load {}: {}/{} chunks done, {} records so far",
                            schema.fullTableName(), startChunk + i + 1, chunks.size(), totalRecords);
                        writer.flush();
                    }
                }

                writer.flush();
                streamState.setFullLoadCompleted(true);
                streamState.setSyncMode("full_load");
                log.info("Full load completed for {}: {} records", schema.fullTableName(), totalRecords);
            } finally {
                executor.shutdown();
            }
        } catch (Exception e) {
            throw new RuntimeException("Full load failed for " + schema.fullTableName(), e);
        }
    }

    @Override
    public void cdcStream(List<CdcSchema> schemas, Writer writer, SyncState state, Runnable checkpointCallback) {
        log.info("Starting CDC stream for {} tables", schemas.size());

        try {
            // Ensure replication infrastructure exists
            ReplicationSlotManager slotManager = new ReplicationSlotManager(managementConnection, config);
            slotManager.ensureSlotExists();

            List<String> tableNames = schemas.stream().map(CdcSchema::fullTableName).toList();
            slotManager.ensurePublicationExists(tableNames);

            // Determine start LSN
            String startLsn = state.getGlobalLsn();
            if (startLsn == null) {
                startLsn = slotManager.getCurrentLsn();
                state.setGlobalLsn(startLsn);
                log.info("No previous LSN found, starting from current: {}", startLsn);
            } else {
                log.info("Resuming CDC from LSN: {}", startLsn);
            }

            // Set up WAL stream
            WalStreamReader walReader = new WalStreamReader(config);
            walReader.start(startLsn);

            PgOutputDecoder decoder = new PgOutputDecoder();
            Set<String> targetTables = new HashSet<>(tableNames);
            final boolean[] running = {true};

            // Register shutdown handler
            Thread shutdownThread = new Thread(() -> {
                running[0] = false;
                log.info("CDC shutdown signal received");
            });
            Runtime.getRuntime().addShutdownHook(shutdownThread);

            try {
                long recordCount = 0;
                long lastFlushTime = System.currentTimeMillis();

                while (running[0]) {
                    ByteBuffer msg = walReader.readPending();
                    if (msg == null) {
                        // No data available, flush if needed
                        long now = System.currentTimeMillis();
                        if (now - lastFlushTime > 5000) {
                            writer.flush();
                            lastFlushTime = now;
                        }
                        Thread.sleep(10);
                        continue;
                    }

                    PgOutputDecoder.DecodedMessage decoded = decoder.decode(msg);
                    if (decoded == null) continue;

                    switch (decoded.type()) {
                        case INSERT, UPDATE, DELETE -> {
                            Record record = decoded.record();
                            if (record != null && targetTables.contains(record.tableName())) {
                                writer.writeRecord(record);
                                recordCount++;

                                if (recordCount % 10000 == 0) {
                                    log.info("CDC: {} records processed", recordCount);
                                }
                            }
                        }
                        case COMMIT -> {
                            writer.flush();
                            lastFlushTime = System.currentTimeMillis();

                            // Update LSN tracking
                            LogSequenceNumber lsn = walReader.getLastReceiveLSN();
                            walReader.setAppliedLsn(lsn);

                            String lsnStr = lsn.asString();
                            state.setGlobalLsn(lsnStr);
                            for (String table : targetTables) {
                                state.getOrCreateStream(table).setLastLsn(lsnStr);
                                state.getOrCreateStream(table).setLastSyncTime(Instant.now());
                                state.getOrCreateStream(table).setSyncMode("cdc");
                            }
                            if (checkpointCallback != null) checkpointCallback.run();
                        }
                        default -> {} // BEGIN, RELATION, TRUNCATE, UNKNOWN
                    }
                }
            } finally {
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownThread);
                } catch (IllegalStateException ignored) {
                    // JVM is already shutting down
                }
                walReader.close();
            }

            log.info("CDC stream ended");
        } catch (Exception e) {
            throw new RuntimeException("CDC streaming failed", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (managementConnection != null && !managementConnection.isClosed()) {
            managementConnection.close();
        }
    }
}
