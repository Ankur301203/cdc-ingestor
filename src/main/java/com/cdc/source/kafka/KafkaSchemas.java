package com.cdc.source.kafka;

import com.cdc.protocol.schema.CdcSchema;
import com.cdc.protocol.schema.CdcType;
import com.cdc.protocol.schema.Column;

import java.util.List;

public final class KafkaSchemas {
    private KafkaSchemas() {}

    public static CdcSchema forTopic(String topic, String destinationTable) {
        return new CdcSchema(topic, destinationTable, envelopeColumns());
    }

    public static List<Column> envelopeColumns() {
        return List.of(
            new Column("_kafka_topic", CdcType.STRING, true, false),
            new Column("_kafka_partition", CdcType.INT32, true, false),
            new Column("_kafka_offset", CdcType.INT64, true, false),
            new Column("_kafka_key", CdcType.STRING, true, false),
            new Column("_kafka_timestamp_ms", CdcType.INT64, true, false),
            new Column("_payload", CdcType.JSON, true, false)
        );
    }
}
