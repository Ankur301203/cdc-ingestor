package com.cdc.protocol;

import java.util.List;

public record RecordBatch(
    List<Record> records,
    long estimatedSizeBytes
) {
    public int size() {
        return records.size();
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }
}
