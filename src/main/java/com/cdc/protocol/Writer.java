package com.cdc.protocol;

import com.cdc.config.DestinationConfig;
import com.cdc.protocol.schema.CdcSchema;

public interface Writer extends AutoCloseable {
    void open(CdcSchema schema, DestinationConfig config);
    void writeRecord(Record record);
    void writeBatch(RecordBatch batch);
    void flush();
    void close();
}
