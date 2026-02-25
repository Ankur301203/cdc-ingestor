package com.cdc.source.postgres;

import com.cdc.config.SourceConfig;
import com.cdc.protocol.CheckResult;
import com.cdc.protocol.Connector;
import com.cdc.protocol.schema.CdcSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

public class PostgresConnector implements Connector {
    private static final Logger log = LoggerFactory.getLogger(PostgresConnector.class);

    @Override
    public String spec() {
        return """
            {
              "type": "object",
              "required": ["host", "port", "database", "username", "password"],
              "properties": {
                "type": { "type": "string", "const": "postgres" },
                "host": { "type": "string", "description": "PostgreSQL host" },
                "port": { "type": "integer", "default": 5432 },
                "database": { "type": "string", "description": "Database name" },
                "username": { "type": "string" },
                "password": { "type": "string" },
                "schemas": { "type": "array", "items": { "type": "string" }, "default": ["public"] },
                "replication": {
                  "type": "object",
                  "properties": {
                    "slot_name": { "type": "string", "default": "cdc_ingestor_slot" },
                    "publication_name": { "type": "string", "default": "cdc_ingestor_pub" },
                    "output_plugin": { "type": "string", "default": "pgoutput" }
                  }
                }
              }
            }
            """;
    }

    @Override
    public CheckResult check(SourceConfig config) {
        try (Connection conn = createConnection(config)) {
            // Check basic connectivity
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT version()")) {
                rs.next();
                String version = rs.getString(1);
                log.info("Connected to: {}", version);
            }

            // Check wal_level
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SHOW wal_level")) {
                rs.next();
                String walLevel = rs.getString(1);
                if (!"logical".equals(walLevel)) {
                    return CheckResult.error("wal_level must be 'logical', currently: " + walLevel);
                }
            }

            // Check replication permissions
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                     "SELECT rolreplication FROM pg_roles WHERE rolname = current_user")) {
                rs.next();
                if (!rs.getBoolean(1)) {
                    return CheckResult.error("Current user does not have replication permission");
                }
            }

            return CheckResult.ok("Connection successful. WAL level: logical, replication enabled.");
        } catch (Exception e) {
            log.error("Connection check failed", e);
            return CheckResult.error("Connection failed: " + e.getMessage());
        }
    }

    @Override
    public List<CdcSchema> discover(SourceConfig config) {
        try (Connection conn = createConnection(config)) {
            PostgresSchemaDiscovery discovery = new PostgresSchemaDiscovery();
            return discovery.discover(conn, config.getSchemas());
        } catch (Exception e) {
            throw new RuntimeException("Schema discovery failed: " + e.getMessage(), e);
        }
    }

    private Connection createConnection(SourceConfig config) throws Exception {
        Properties props = new Properties();
        props.setProperty("user", config.getUsername());
        props.setProperty("password", config.getPassword());
        return DriverManager.getConnection(config.jdbcUrl(), props);
    }
}
