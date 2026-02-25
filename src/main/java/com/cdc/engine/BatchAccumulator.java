package com.cdc.engine;

import com.cdc.protocol.Record;
import com.cdc.protocol.RecordBatch;

import java.util.ArrayList;
import java.util.List;

public class BatchAccumulator {
    private static final int DEFAULT_BATCH_SIZE = 10000;

    private final int batchSize;
    private final List<Record> records;

    public BatchAccumulator() {
        this(DEFAULT_BATCH_SIZE);
    }

    public BatchAccumulator(int batchSize) {
        this.batchSize = batchSize;
        this.records = new ArrayList<>(batchSize);
    }

    public void add(Record record) {
        records.add(record);
    }

    public boolean isFull() {
        return records.size() >= batchSize;
    }

    public RecordBatch flush() {
        if (records.isEmpty()) return null;
        RecordBatch batch = new RecordBatch(new ArrayList<>(records), estimateSize());
        records.clear();
        return batch;
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    public int size() {
        return records.size();
    }

    private long estimateSize() {
        // Rough estimate: ~200 bytes per record
        return (long) records.size() * 200;
    }
}
