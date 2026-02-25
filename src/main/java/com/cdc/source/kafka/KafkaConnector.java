package com.cdc.source.kafka;

import com.cdc.config.SourceConfig;
import com.cdc.protocol.CheckResult;
import com.cdc.protocol.Connector;
import com.cdc.protocol.schema.CdcSchema;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class KafkaConnector implements Connector {
    private static final Logger log = LoggerFactory.getLogger(KafkaConnector.class);

    @Override
    public String spec() {
        return """
            {
              "type": "object",
              "required": ["bootstrap_servers"],
              "properties": {
                "type": { "type": "string", "const": "kafka" },
                "bootstrap_servers": { "type": "string", "description": "Kafka bootstrap servers, e.g. host1:9092,host2:9092" },
                "group_id": { "type": "string", "default": "cdc-ingestor" },
                "topics": { "type": "array", "items": { "type": "string" }, "description": "Optional: limit discovery to these topics" },
                "auto_offset_reset": { "type": "string", "enum": ["latest", "earliest"], "default": "latest" },
                "poll_timeout_ms": { "type": "integer", "default": 1000 }
              }
            }
            """;
    }

    @Override
    public CheckResult check(SourceConfig config) {
        if (config.getBootstrapServers() == null || config.getBootstrapServers().isBlank()) {
            return CheckResult.error("bootstrap_servers is required for kafka source");
        }

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());

        try (AdminClient admin = AdminClient.create(props)) {
            Set<String> topics = admin.listTopics(new ListTopicsOptions().timeoutMs(5000))
                .names()
                .get(5, TimeUnit.SECONDS);
            log.info("Kafka connection OK. Visible topics: {}", topics.size());
            return CheckResult.ok("Connection successful. Visible topics: " + topics.size());
        } catch (Exception e) {
            log.error("Kafka connection check failed", e);
            return CheckResult.error("Connection failed: " + e.getMessage());
        }
    }

    @Override
    public List<CdcSchema> discover(SourceConfig config) {
        if (config.getBootstrapServers() == null || config.getBootstrapServers().isBlank()) {
            throw new IllegalArgumentException("bootstrap_servers is required for kafka discovery");
        }

        List<String> configured = config.getTopics() != null ? config.getTopics() : List.of();
        List<CdcSchema> out = new ArrayList<>();

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());

        try (AdminClient admin = AdminClient.create(props)) {
            Set<String> topics = admin.listTopics(new ListTopicsOptions()
                    .timeoutMs((int) Duration.ofSeconds(10).toMillis())
                    .listInternal(false))
                .names()
                .get(10, TimeUnit.SECONDS);

            for (String topic : topics) {
                if (!configured.isEmpty() && !configured.contains(topic)) continue;
                // For discover output, treat "kafka.<topic>" as the stream id.
                out.add(KafkaSchemas.forTopic(topic, "kafka_" + topic.replace('.', '_')));
            }
            return out;
        } catch (Exception e) {
            throw new RuntimeException("Kafka discovery failed: " + e.getMessage(), e);
        }
    }
}

