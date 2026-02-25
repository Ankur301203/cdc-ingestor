package com.cdc.source;

import com.cdc.config.SourceConfig;
import com.cdc.protocol.Driver;
import com.cdc.source.kafka.KafkaDriver;
import com.cdc.source.postgres.PostgresDriver;

public final class SourceDriverFactory {
    private SourceDriverFactory() {}

    public static Driver forConfig(SourceConfig config) {
        if (config == null) throw new IllegalArgumentException("source config is required");
        return forType(config.getType());
    }

    public static Driver forType(String type) {
        String t = type == null ? "postgres" : type.trim().toLowerCase();
        return switch (t) {
            case "postgres" -> new PostgresDriver();
            case "kafka" -> new KafkaDriver();
            default -> throw new IllegalArgumentException("Unsupported source type: " + type);
        };
    }
}

