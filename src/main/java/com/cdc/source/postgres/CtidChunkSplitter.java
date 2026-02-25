package com.cdc.source.postgres;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class CtidChunkSplitter {
    private static final Logger log = LoggerFactory.getLogger(CtidChunkSplitter.class);
    private static final int DEFAULT_PAGES_PER_CHUNK = 500;

    public record CtidRange(long startPage, long endPage) {
        public String toWhereClause(String tableName) {
            return String.format(
                "ctid >= '(%d,0)'::tid AND ctid < '(%d,0)'::tid",
                startPage, endPage);
        }
    }

    public List<CtidRange> split(Connection connection, String schemaName, String tableName) throws SQLException {
        return split(connection, schemaName, tableName, DEFAULT_PAGES_PER_CHUNK);
    }

    public List<CtidRange> split(Connection connection, String schemaName, String tableName,
                                  int pagesPerChunk) throws SQLException {
        long totalPages = getTotalPages(connection, schemaName, tableName);
        log.info("Table {}.{} has {} pages, splitting into chunks of {} pages",
                schemaName, tableName, totalPages, pagesPerChunk);

        List<CtidRange> chunks = new ArrayList<>();
        for (long start = 0; start < totalPages; start += pagesPerChunk) {
            long end = Math.min(start + pagesPerChunk, totalPages + 1);
            chunks.add(new CtidRange(start, end));
        }

        if (chunks.isEmpty()) {
            // Table might be empty but still run one chunk
            chunks.add(new CtidRange(0, 1));
        }

        log.info("Split {}.{} into {} chunks", schemaName, tableName, chunks.size());
        return chunks;
    }

    private long getTotalPages(Connection connection, String schemaName, String tableName) throws SQLException {
        String sql = """
            SELECT relpages FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = ? AND c.relname = ?
            """;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, schemaName);
            ps.setString(2, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("relpages");
                }
                return 0;
            }
        }
    }
}
