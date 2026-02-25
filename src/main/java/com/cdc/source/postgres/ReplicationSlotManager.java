package com.cdc.source.postgres;

import com.cdc.config.SourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

public class ReplicationSlotManager {
    private static final Logger log = LoggerFactory.getLogger(ReplicationSlotManager.class);

    private final Connection connection;
    private final SourceConfig config;

    public ReplicationSlotManager(Connection connection, SourceConfig config) {
        this.connection = connection;
        this.config = config;
    }

    public void ensureSlotExists() throws SQLException {
        String slotName = config.getReplication().getSlotName();
        String plugin = config.getReplication().getOutputPlugin();

        if (!slotExists(slotName)) {
            log.info("Creating replication slot: {}", slotName);
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(String.format(
                    "SELECT pg_create_logical_replication_slot('%s', '%s')",
                    slotName, plugin));
            }
            log.info("Replication slot '{}' created", slotName);
        } else {
            log.info("Replication slot '{}' already exists", slotName);
        }
    }

    public void ensurePublicationExists(List<String> tables) throws SQLException {
        String pubName = config.getReplication().getPublicationName();

        if (!publicationExists(pubName)) {
            String tableList = String.join(", ", tables);
            log.info("Creating publication '{}' for tables: {}", pubName, tableList);
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(String.format(
                    "CREATE PUBLICATION %s FOR TABLE %s", pubName, tableList));
            }
            log.info("Publication '{}' created", pubName);
        } else {
            log.info("Publication '{}' already exists", pubName);
        }
    }

    public void dropSlot() throws SQLException {
        String slotName = config.getReplication().getSlotName();
        if (slotExists(slotName)) {
            log.info("Dropping replication slot: {}", slotName);
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(String.format(
                    "SELECT pg_drop_replication_slot('%s')", slotName));
            }
        }
    }

    public String getCurrentLsn() throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT pg_current_wal_lsn()::text")) {
            rs.next();
            return rs.getString(1);
        }
    }

    private boolean slotExists(String slotName) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, slotName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private boolean publicationExists(String pubName) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT 1 FROM pg_publication WHERE pubname = ?")) {
            ps.setString(1, pubName);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }
}
