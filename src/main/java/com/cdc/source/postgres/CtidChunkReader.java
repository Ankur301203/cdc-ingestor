package com.cdc.source.postgres;

import com.cdc.protocol.OperationType;
import com.cdc.protocol.Record;
import com.cdc.protocol.schema.CdcSchema;
import com.cdc.protocol.schema.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Callable;

public class CtidChunkReader implements Callable<List<Record>> {
    private static final Logger log = LoggerFactory.getLogger(CtidChunkReader.class);

    private final String jdbcUrl;
    private final Properties connectionProps;
    private final CdcSchema schema;
    private final CtidChunkSplitter.CtidRange range;
    private final int chunkIndex;

    public CtidChunkReader(String jdbcUrl, Properties connectionProps, CdcSchema schema,
                           CtidChunkSplitter.CtidRange range, int chunkIndex) {
        this.jdbcUrl = jdbcUrl;
        this.connectionProps = connectionProps;
        this.schema = schema;
        this.range = range;
        this.chunkIndex = chunkIndex;
    }

    @Override
    public List<Record> call() throws Exception {
        List<Record> records = new ArrayList<>();
        String fullTable = schema.fullTableName();
        String whereClause = range.toWhereClause(fullTable);

        String columnList = String.join(", ",
            schema.columns().stream().map(Column::name).toList());

        String sql = String.format("SELECT %s FROM %s WHERE %s", columnList, fullTable, whereClause);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, connectionProps)) {
            conn.setAutoCommit(false);
            conn.setReadOnly(true);
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

            try (Statement stmt = conn.createStatement()) {
                stmt.setFetchSize(5000);
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    ResultSetMetaData meta = rs.getMetaData();
                    int columnCount = meta.getColumnCount();

                    while (rs.next()) {
                        Map<String, Object> data = new LinkedHashMap<>();
                        for (int i = 1; i <= columnCount; i++) {
                            String colName = meta.getColumnName(i);
                            Object value = rs.getObject(i);
                            data.put(colName, value);
                        }
                        records.add(new Record(
                            fullTable,
                            OperationType.SNAPSHOT,
                            data,
                            null,
                            0,
                            Instant.now()
                        ));
                    }
                }
            }
        }

        log.debug("Chunk {} of {}: read {} records", chunkIndex, fullTable, records.size());
        return records;
    }
}
