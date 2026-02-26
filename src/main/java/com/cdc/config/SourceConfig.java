package com.cdc.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SourceConfig {
    private String type = "postgres";

    // postgres
    private String host = "localhost";
    private int port = 5432;
    private String database;
    private String username;
    private String password;
    private List<String> schemas = List.of("public");
    private ReplicationConfig replication = new ReplicationConfig();

    // kafka
    @JsonProperty("bootstrap_servers")
    private String bootstrapServers;

    @JsonProperty("group_id")
    private String groupId = "cdc-ingestor";

    private List<String> topics = List.of();

    @JsonProperty("auto_offset_reset")
    private String autoOffsetReset = "latest";

    @JsonProperty("poll_timeout_ms")
    private int pollTimeoutMs = 1000;

    @JsonProperty("checkpoint_interval_ms")
    private int checkpointIntervalMs = 5000;

    @JsonProperty("checkpoint_records")
    private int checkpointRecords = 10000;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ReplicationConfig {
        @JsonProperty("slot_name")
        private String slotName = "cdc_ingestor_slot";

        @JsonProperty("publication_name")
        private String publicationName = "cdc_ingestor_pub";

        @JsonProperty("output_plugin")
        private String outputPlugin = "pgoutput";

        public String getSlotName() { return slotName; }
        public void setSlotName(String slotName) { this.slotName = slotName; }
        public String getPublicationName() { return publicationName; }
        public void setPublicationName(String publicationName) { this.publicationName = publicationName; }
        public String getOutputPlugin() { return outputPlugin; }
        public void setOutputPlugin(String outputPlugin) { this.outputPlugin = outputPlugin; }
    }

    public String jdbcUrl() {
        return String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
    }

    public String replicationUrl() {
        return jdbcUrl() + "?reWriteBatchedInserts=true&assumeMinServerVersion=9.4";
    }

    // Getters and setters
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }
    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }
    public String getDatabase() { return database; }
    public void setDatabase(String database) { this.database = database; }
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }
    public List<String> getSchemas() { return schemas; }
    public void setSchemas(List<String> schemas) { this.schemas = schemas; }
    public ReplicationConfig getReplication() { return replication; }
    public void setReplication(ReplicationConfig replication) { this.replication = replication; }

    public String getBootstrapServers() { return bootstrapServers; }
    public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }
    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }
    public List<String> getTopics() { return topics; }
    public void setTopics(List<String> topics) { this.topics = topics; }
    public String getAutoOffsetReset() { return autoOffsetReset; }
    public void setAutoOffsetReset(String autoOffsetReset) { this.autoOffsetReset = autoOffsetReset; }
    public int getPollTimeoutMs() { return pollTimeoutMs; }
    public void setPollTimeoutMs(int pollTimeoutMs) { this.pollTimeoutMs = pollTimeoutMs; }
    public int getCheckpointIntervalMs() { return checkpointIntervalMs; }
    public void setCheckpointIntervalMs(int checkpointIntervalMs) { this.checkpointIntervalMs = checkpointIntervalMs; }
    public int getCheckpointRecords() { return checkpointRecords; }
    public void setCheckpointRecords(int checkpointRecords) { this.checkpointRecords = checkpointRecords; }
}
