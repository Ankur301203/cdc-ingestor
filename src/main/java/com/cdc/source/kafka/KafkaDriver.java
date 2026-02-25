package com.cdc.source.kafka;

import com.cdc.config.SourceConfig;
import com.cdc.protocol.Driver;
import com.cdc.protocol.OperationType;
import com.cdc.protocol.Record;
import com.cdc.protocol.Writer;
import com.cdc.protocol.schema.CdcSchema;
import com.cdc.protocol.schema.Column;
import com.cdc.protocol.schema.CdcType;
import com.cdc.state.SyncState;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaDriver implements Driver {
    private static final Logger log = LoggerFactory.getLogger(KafkaDriver.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SourceConfig config;
    private List<CdcSchema> schemas;
    private KafkaConsumer<String, String> consumer;

    @Override
    public void init(SourceConfig config, List<CdcSchema> schemas) {
        this.config = config;
        this.schemas = schemas;
    }

    @Override
    public void fullLoad(CdcSchema schema, Writer writer, SyncState state) {
        throw new UnsupportedOperationException("Kafka source does not support full_load");
    }

    @Override
    public void cdcStream(List<CdcSchema> schemas, Writer writer, SyncState state, Runnable shutdownSignal) {
        if (schemas == null || schemas.isEmpty()) {
            throw new IllegalArgumentException("Kafka CDC requires at least one stream");
        }
        if (schemas.size() != 1) {
            throw new IllegalArgumentException("Kafka CDC currently supports exactly 1 stream (one topic -> one destination table)");
        }

        CdcSchema schema = schemas.get(0);
        String topic = schema.schemaName();
        String destinationTable = schema.tableName();
        String streamId = schema.fullTableName();
        List<Column> payloadColumns = schema.columns().stream()
            .filter(c -> !c.name().startsWith("_kafka_"))
            .filter(c -> !"_payload".equals(c.name()))
            .collect(Collectors.toList());

        if (config.getBootstrapServers() == null || config.getBootstrapServers().isBlank()) {
            throw new IllegalArgumentException("bootstrap_servers is required for kafka source");
        }

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getAutoOffsetReset());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topic));

        final boolean[] running = {true};
        Thread shutdownThread = new Thread(() -> {
            running[0] = false;
            log.info("Kafka CDC shutdown signal received");
        });
        Runtime.getRuntime().addShutdownHook(shutdownThread);

        try {
            waitForAssignmentAndSeek(state, streamId, topic);

            long recordCount = 0;
            long lastFlushTime = System.currentTimeMillis();

            while (running[0]) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(config.getPollTimeoutMs()));
                if (records.isEmpty()) {
                    long now = System.currentTimeMillis();
                    if (now - lastFlushTime > 5000) {
                        writer.flush();
                        lastFlushTime = now;
                    }
                    continue;
                }

                Map<Integer, Long> maxOffsetByPartition = new HashMap<>();

                for (ConsumerRecord<String, String> rec : records) {
                    recordCount++;

                    Map<String, Object> data = new HashMap<>();
                    data.put("_kafka_topic", rec.topic());
                    data.put("_kafka_partition", rec.partition());
                    data.put("_kafka_offset", rec.offset());
                    data.put("_kafka_key", rec.key());
                    data.put("_kafka_timestamp_ms", rec.timestamp());
                    data.put("_payload", rec.value());

                    if (!payloadColumns.isEmpty() && rec.value() != null && !rec.value().isBlank()) {
                        try {
                            JsonNode root = MAPPER.readTree(rec.value());
                            for (Column col : payloadColumns) {
                                JsonNode node = root != null ? root.get(col.name()) : null;
                                data.put(col.name(), convertJson(node, col.type()));
                            }
                        } catch (Exception e) {
                            log.warn("Failed to parse JSON payload; leaving normalized columns null. error={}", e.getMessage());
                        }
                    }

                    Instant ts = rec.timestamp() > 0 ? Instant.ofEpochMilli(rec.timestamp()) : Instant.now();
                    Record out = new Record(destinationTable, OperationType.INSERT, data, null, rec.offset(), ts);
                    writer.writeRecord(out);

                    maxOffsetByPartition.merge(rec.partition(), rec.offset(), Math::max);

                    if (recordCount % 10000 == 0) {
                        log.info("Kafka CDC: {} records processed", recordCount);
                    }
                }

                writer.flush();
                consumer.commitSync();
                lastFlushTime = System.currentTimeMillis();

                SyncState.StreamState streamState = state.getOrCreateStream(streamId);
                streamState.setSyncMode("cdc");
                streamState.setLastSyncTime(Instant.now());

                Map<String, Map<Integer, Long>> offsets = streamState.getKafkaOffsets();
                if (offsets == null) offsets = new HashMap<>();
                offsets.put(topic, maxOffsetByPartition);
                streamState.setKafkaOffsets(offsets);
            }
        } finally {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownThread);
            } catch (IllegalStateException ignored) {
                // JVM is already shutting down
            }
            try {
                writer.flush();
            } catch (Exception ignored) {
            }
            closeConsumerQuietly();
        }
    }

    private void waitForAssignmentAndSeek(SyncState state, String streamId, String topic) {
        // poll until assigned partitions
        for (int i = 0; i < 50; i++) {
            consumer.poll(Duration.ofMillis(100));
            Set<TopicPartition> assigned = consumer.assignment();
            if (!assigned.isEmpty()) {
                applySeeksFromState(state, streamId, topic, assigned);
                log.info("Kafka CDC assigned partitions: {}", assigned);
                return;
            }
        }
        log.warn("Kafka CDC did not receive partition assignment in time; starting with consumer defaults");
    }

    private void applySeeksFromState(SyncState state, String streamId, String topic, Set<TopicPartition> assigned) {
        SyncState.StreamState streamState = state.getOrCreateStream(streamId);
        Map<String, Map<Integer, Long>> offsets = streamState.getKafkaOffsets();
        if (offsets == null) return;
        Map<Integer, Long> byPartition = offsets.get(topic);
        if (byPartition == null) return;

        for (TopicPartition tp : assigned) {
            Long lastProcessed = byPartition.get(tp.partition());
            if (lastProcessed == null) continue;
            long nextOffset = lastProcessed + 1;
            consumer.seek(tp, nextOffset);
            log.info("Seeking {} to offset {}", tp, nextOffset);
        }
    }

    private void closeConsumerQuietly() {
        if (consumer == null) return;
        try {
            consumer.close(Duration.ofSeconds(5));
        } catch (Exception e) {
            log.warn("Failed to close Kafka consumer cleanly: {}", e.getMessage());
        }
        consumer = null;
    }

    private Object convertJson(JsonNode node, CdcType type) {
        if (node == null || node.isNull()) return null;
        return switch (type) {
            case INT32 -> node.isNumber() ? node.intValue() : Integer.parseInt(node.asText());
            case INT64 -> node.isNumber() ? node.longValue() : Long.parseLong(node.asText());
            case FLOAT -> node.isNumber() ? node.floatValue() : Float.parseFloat(node.asText());
            case DOUBLE -> node.isNumber() ? node.doubleValue() : Double.parseDouble(node.asText());
            case BOOLEAN -> node.isBoolean() ? node.booleanValue() : Boolean.parseBoolean(node.asText());
            case DECIMAL -> new BigDecimal(node.asText());
            case JSON, ARRAY -> node.toString();
            // Keep these as strings; IcebergWriter will parse for temporal types if needed.
            case DATE, TIME, TIMESTAMP, TIMESTAMP_TZ, STRING, UUID, BYTES -> node.asText();
        };
    }

    @Override
    public void close() {
        closeConsumerQuietly();
    }
}
