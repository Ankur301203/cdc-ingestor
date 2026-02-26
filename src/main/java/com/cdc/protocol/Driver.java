package com.cdc.protocol;

import com.cdc.config.SourceConfig;
import com.cdc.protocol.schema.CdcSchema;
import com.cdc.state.SyncState;

import java.util.List;

public interface Driver extends AutoCloseable {
    void init(SourceConfig config, List<CdcSchema> schemas);
    void fullLoad(CdcSchema schema, Writer writer, SyncState state);
    void cdcStream(List<CdcSchema> schemas, Writer writer, SyncState state, Runnable checkpointCallback);
}
