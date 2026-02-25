package com.cdc.source.postgres;

import com.cdc.protocol.schema.CdcSchema;
import com.cdc.protocol.schema.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class PostgresSchemaDiscovery {
    private static final Logger log = LoggerFactory.getLogger(PostgresSchemaDiscovery.class);

    public List<CdcSchema> discover(Connection connection, List<String> schemas) throws SQLException {
        List<CdcSchema> result = new ArrayList<>();

        for (String schemaName : schemas) {
            Map<String, Set<String>> primaryKeys = discoverPrimaryKeys(connection, schemaName);
            List<CdcSchema> tableSchemas = discoverTables(connection, schemaName, primaryKeys);
            result.addAll(tableSchemas);
        }

        log.info("Discovered {} tables across {} schemas", result.size(), schemas.size());
        return result;
    }

    private Map<String, Set<String>> discoverPrimaryKeys(Connection connection, String schemaName) throws SQLException {
        Map<String, Set<String>> pkMap = new HashMap<>();

        String sql = """
            SELECT tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = ?
            ORDER BY tc.table_name, kcu.ordinal_position
            """;

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String table = rs.getString("table_name");
                    String column = rs.getString("column_name");
                    pkMap.computeIfAbsent(table, k -> new LinkedHashSet<>()).add(column);
                }
            }
        }
        return pkMap;
    }

    private List<CdcSchema> discoverTables(Connection connection, String schemaName,
                                            Map<String, Set<String>> primaryKeys) throws SQLException {
        Map<String, List<Column>> tableColumns = new LinkedHashMap<>();

        String sql = """
            SELECT table_name, column_name, data_type, udt_name, is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = ?
            ORDER BY table_name, ordinal_position
            """;

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("table_name");
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    String udtName = rs.getString("udt_name");
                    boolean nullable = "YES".equals(rs.getString("is_nullable"));

                    // Use udt_name for better type resolution (handles user-defined and array types)
                    String effectiveType = "USER-DEFINED".equals(dataType) ? udtName : dataType;
                    Set<String> pks = primaryKeys.getOrDefault(tableName, Set.of());
                    boolean isPk = pks.contains(columnName);

                    Column column = new Column(columnName, PostgresTypeMapper.map(effectiveType), nullable, isPk);
                    tableColumns.computeIfAbsent(tableName, k -> new ArrayList<>()).add(column);
                }
            }
        }

        // Filter out system tables
        List<CdcSchema> schemas = new ArrayList<>();
        for (var entry : tableColumns.entrySet()) {
            String tableName = entry.getKey();
            if (tableName.startsWith("pg_") || tableName.startsWith("sql_")) continue;
            schemas.add(new CdcSchema(schemaName, tableName, entry.getValue()));
        }
        return schemas;
    }
}
