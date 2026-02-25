package com.cdc.protocol;

import com.cdc.config.SourceConfig;
import com.cdc.protocol.schema.CdcSchema;

import java.util.List;

public interface Connector {
    String spec();
    CheckResult check(SourceConfig config);
    List<CdcSchema> discover(SourceConfig config);
}
