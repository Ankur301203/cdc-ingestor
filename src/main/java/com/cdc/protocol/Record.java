package com.cdc.protocol;

import java.time.Instant;
import java.util.Map;

public record Record(
    String tableName,
    OperationType operation,
    Map<String, Object> data,
    Map<String, Object> beforeData,
    long lsn,
    Instant timestamp
) {
    public Record(String tableName, OperationType operation, Map<String, Object> data, long lsn) {
        this(tableName, operation, data, null, lsn, Instant.now());
    }
}
