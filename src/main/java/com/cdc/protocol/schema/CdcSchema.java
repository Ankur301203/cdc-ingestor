package com.cdc.protocol.schema;

import java.util.List;

public record CdcSchema(
    String schemaName,
    String tableName,
    List<Column> columns
) {
    public String fullTableName() {
        return schemaName + "." + tableName;
    }

    public List<Column> primaryKeyColumns() {
        return columns.stream().filter(Column::primaryKey).toList();
    }
}
