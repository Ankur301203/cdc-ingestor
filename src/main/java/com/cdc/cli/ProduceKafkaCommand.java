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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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

    @Option(names = "--threads", description = "Number of producer threads", defaultValue = "4")
    private int threads;

    @Option(names = "--print-every", description = "Print progress every N events (0 disables)", defaultValue = "5000")
    private int printEvery;

    @Option(names = "--no-print", description = "Disable progress printing")
    private boolean noPrint;

    @Override
    public void run() {
        try {
            String servers = resolveBootstrapServers();
            if (servers == null || servers.isBlank()) {
                throw new IllegalArgumentException("bootstrap servers required via --bootstrap-servers or --source");
            }
            if (count < 0) {
                throw new IllegalArgumentException("--count must be >= 0");
            }
            if (threads <= 0) {
                throw new IllegalArgumentException("--threads must be >= 1");
            }
            if (printEvery < 0) {
                throw new IllegalArgumentException("--print-every must be >= 0");
            }
            if (count == 0) {
                System.out.println("No events to publish (--count=0)");
                return;
            }
            if (noPrint) printEvery = 0;

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                long start = System.currentTimeMillis();
                CountDownLatch latch = new CountDownLatch(count);
                AtomicInteger ok = new AtomicInteger();
                AtomicInteger failed = new AtomicInteger();
                AtomicInteger done = new AtomicInteger();

                int workerCount = Math.min(threads, count);
                ExecutorService exec = Executors.newFixedThreadPool(workerCount, r -> {
                    Thread t = new Thread(r, "kafka-producer");
                    t.setDaemon(false);
                    return t;
                });

                List<Future<?>> futures = new ArrayList<>(workerCount);
                AtomicInteger seq = new AtomicInteger(0);

                for (int w = 0; w < workerCount; w++) {
                    futures.add(exec.submit(() -> {
                        while (true) {
                            int i = seq.incrementAndGet();
                            if (i > count) break;

                            try {
                                Map<String, Object> payload = new LinkedHashMap<>();
                                payload.put("user_id", 1000L + i);
                                payload.put("event_type", "test");
                                payload.put("created_at", Instant.now().toString());
                                payload.put("seq", i);
                                payload.put("message", "hello from cdc-ingestor");

                                String value = MAPPER.writeValueAsString(payload);
                                String key = String.valueOf(payload.get("user_id"));

                                producer.send(new ProducerRecord<>(topic, key, value), (metadata, exception) -> {
                                    try {
                                        if (exception != null) {
                                            failed.incrementAndGet();
                                            System.err.println("publish failed topic=" + topic + " key=" + key + " error=" + exception.getMessage());
                                        } else {
                                            ok.incrementAndGet();
                                        }
                                        int d = done.incrementAndGet();
                                        if (printEvery > 0 && (d % printEvery) == 0) {
                                            System.out.println("published progress topic=" + topic + " done=" + d + " ok=" + ok.get() + " failed=" + failed.get());
                                        }
                                    } finally {
                                        latch.countDown();
                                    }
                                });
                            } catch (Exception e) {
                                failed.incrementAndGet();
                                System.err.println("publish failed topic=" + topic + " seq=" + i + " error=" + e.getMessage());
                                int d = done.incrementAndGet();
                                if (printEvery > 0 && (d % printEvery) == 0) {
                                    System.out.println("published progress topic=" + topic + " done=" + d + " ok=" + ok.get() + " failed=" + failed.get());
                                }
                                latch.countDown();
                            }

                            if (intervalMs > 0) {
                                try {
                                    Thread.sleep(intervalMs);
                                } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                    break;
                                }
                            }
                        }
                    }));
                }

                for (Future<?> f : futures) {
                    f.get();
                }
                latch.await(5, TimeUnit.MINUTES);
                producer.flush();
                exec.shutdown();

                long ms = System.currentTimeMillis() - start;
                System.out.println("publish complete topic=" + topic + " ok=" + ok.get() + " failed=" + failed.get() + " ms=" + ms);
                if (failed.get() > 0) System.exit(1);
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
