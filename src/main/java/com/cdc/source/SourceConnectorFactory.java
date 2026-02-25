package com.cdc.source;

import com.cdc.config.SourceConfig;
import com.cdc.protocol.Connector;
import com.cdc.source.kafka.KafkaConnector;
import com.cdc.source.postgres.PostgresConnector;

public final class SourceConnectorFactory {
    private SourceConnectorFactory() {}

    public static Connector forConfig(SourceConfig config) {
        if (config == null) throw new IllegalArgumentException("source config is required");
        return forType(config.getType());
    }

    public static Connector forType(String type) {
        String t = type == null ? "postgres" : type.trim().toLowerCase();
        return switch (t) {
            case "postgres" -> new PostgresConnector();
            case "kafka" -> new KafkaConnector();
            default -> throw new IllegalArgumentException("Unsupported source type: " + type);
        };
    }
}

