package com.cdc.source.kafka;

import com.cdc.config.PayloadColumnConfig;
import com.cdc.config.StreamEntry;
import com.cdc.protocol.schema.CdcSchema;
import com.cdc.protocol.schema.CdcType;
import com.cdc.protocol.schema.Column;

import java.util.ArrayList;
import java.util.List;

public final class KafkaSchemas {
    private KafkaSchemas() {}

    public static CdcSchema forTopic(String topic, String destinationTable) {
        return new CdcSchema(topic, destinationTable, envelopeColumns());
    }

    public static CdcSchema forTopic(String topic, String destinationTable, StreamEntry entry) {
        List<Column> columns = new ArrayList<>(envelopeColumnsBase());
        columns.add(new Column("_payload", CdcType.JSON, true, false));

        boolean normalize = entry != null
            && entry.isNormalizePayload()
            && entry.getPayloadColumns() != null
            && !entry.getPayloadColumns().isEmpty();

        if (normalize) {
            for (PayloadColumnConfig c : entry.getPayloadColumns()) {
                if (c == null || c.getName() == null || c.getName().isBlank()) continue;
                CdcType type = c.getType() != null ? c.getType() : CdcType.STRING;
                columns.add(new Column(c.getName(), type, c.isNullable(), false));
            }
        }

        return new CdcSchema(topic, destinationTable, columns);
    }

    public static List<Column> envelopeColumns() {
        List<Column> cols = new ArrayList<>(envelopeColumnsBase());
        cols.add(new Column("_payload", CdcType.JSON, true, false));
        return cols;
    }

    private static List<Column> envelopeColumnsBase() {
        return List.of(
            new Column("_kafka_topic", CdcType.STRING, true, false),
            new Column("_kafka_partition", CdcType.INT32, true, false),
            new Column("_kafka_offset", CdcType.INT64, true, false),
            new Column("_kafka_key", CdcType.STRING, true, false),
            new Column("_kafka_timestamp_ms", CdcType.INT64, true, false)
        );
    }
}
