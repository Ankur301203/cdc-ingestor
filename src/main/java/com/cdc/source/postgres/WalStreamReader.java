package com.cdc.source.postgres;

import com.cdc.config.SourceConfig;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WalStreamReader implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(WalStreamReader.class);

    private final SourceConfig config;
    private Connection replicationConnection;
    private PGReplicationStream stream;

    public WalStreamReader(SourceConfig config) {
        this.config = config;
    }

    public void start(String startLsn) throws Exception {
        Properties props = new Properties();
        props.setProperty("user", config.getUsername());
        props.setProperty("password", config.getPassword());
        props.setProperty("replication", "database");
        props.setProperty("assumeMinServerVersion", "9.4");

        replicationConnection = DriverManager.getConnection(config.jdbcUrl(), props);
        PGConnection pgConnection = replicationConnection.unwrap(PGConnection.class);

        LogSequenceNumber lsn = startLsn != null
            ? LogSequenceNumber.valueOf(startLsn)
            : LogSequenceNumber.INVALID_LSN;

        stream = pgConnection.getReplicationAPI()
            .replicationStream()
            .logical()
            .withSlotName(config.getReplication().getSlotName())
            .withStartPosition(lsn)
            .withSlotOption("proto_version", "1")
            .withSlotOption("publication_names", config.getReplication().getPublicationName())
            .start();

        log.info("WAL stream started from LSN: {}", startLsn != null ? startLsn : "latest");
    }

    public ByteBuffer readPending() throws Exception {
        return stream.readPending();
    }

    public ByteBuffer read(long timeout, TimeUnit unit) throws Exception {
        return stream.read();
    }

    public LogSequenceNumber getLastReceiveLSN() {
        return stream.getLastReceiveLSN();
    }

    public void setAppliedLsn(LogSequenceNumber lsn) {
        stream.setAppliedLSN(lsn);
        stream.setFlushedLSN(lsn);
    }

    @Override
    public void close() throws Exception {
        if (stream != null) {
            stream.close();
        }
        if (replicationConnection != null) {
            replicationConnection.close();
        }
        log.info("WAL stream closed");
    }
}
