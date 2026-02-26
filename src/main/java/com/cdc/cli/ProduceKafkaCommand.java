package com.cdc.cli;

import com.cdc.config.ConfigLoader;
import com.cdc.config.SourceConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

@Command(name = "produce-kafka", description = "Publish sample JSON events to a Kafka topic (testing helper)")
public class ProduceKafkaCommand implements Runnable {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Option(names = "--source", description = "Optional: Path to kafka source config YAML (uses bootstrap_servers)")
    private Path sourcePath;

    @Option(names = "--bootstrap-servers", description = "Kafka bootstrap servers (overrides --source)")
    private String bootstrapServers;

    @Option(names = "--topic", description = "Topic to publish to", defaultValue = "test-topic")
    private String topic;

    @Option(names = "--count", description = "Number of events to publish", defaultValue = "10")
    private int count;

    @Option(names = "--interval-ms", description = "Sleep between events", defaultValue = "0")
    private long intervalMs;

    @Override
    public void run() {
        try {
            String servers = resolveBootstrapServers();
            if (servers == null || servers.isBlank()) {
                throw new IllegalArgumentException("bootstrap servers required via --bootstrap-servers or --source");
            }

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                for (int i = 1; i <= count; i++) {
                    Map<String, Object> payload = new LinkedHashMap<>();
                    payload.put("user_id", 1000L + i);
                    payload.put("event_type", "test");
                    payload.put("created_at", Instant.now().toString());
                    payload.put("seq", i);
                    payload.put("message", "hello from cdc-ingestor");

                    String value = MAPPER.writeValueAsString(payload);
                    String key = String.valueOf(payload.get("user_id"));

                    producer.send(new ProducerRecord<>(topic, key, value)).get();
                    System.out.println("published topic=" + topic + " key=" + key + " value=" + value);

                    if (intervalMs > 0) Thread.sleep(intervalMs);
                }
                producer.flush();
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    private String resolveBootstrapServers() throws Exception {
        if (bootstrapServers != null && !bootstrapServers.isBlank()) return bootstrapServers;
        if (sourcePath == null) return null;
        SourceConfig cfg = ConfigLoader.loadSource(sourcePath);
        return cfg.getBootstrapServers();
    }
}

