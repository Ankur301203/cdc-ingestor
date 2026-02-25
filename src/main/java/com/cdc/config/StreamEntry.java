package com.cdc.config;

import com.cdc.protocol.SyncMode;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StreamEntry {
    private String table;

    @JsonProperty("source_topic")
    private String sourceTopic;

    @JsonProperty("sync_mode")
    private String syncMode = "full_load+cdc";

    @JsonProperty("normalize_payload")
    private boolean normalizePayload;

    @JsonProperty("payload_columns")
    private List<PayloadColumnConfig> payloadColumns = List.of();

    @JsonProperty("cursor_field")
    private String cursorField;

    @JsonProperty("primary_key")
    private List<String> primaryKey = List.of();

    @JsonProperty("destination_table")
    private String destinationTable;

    public SyncMode parseSyncMode() {
        return switch (syncMode.toLowerCase()) {
            case "full_load" -> SyncMode.FULL_LOAD;
            case "cdc" -> SyncMode.CDC;
            case "full_load+cdc" -> SyncMode.FULL_LOAD_CDC;
            default -> SyncMode.FULL_LOAD_CDC;
        };
    }

    public String getEffectiveDestinationTable() {
        if (destinationTable != null && !destinationTable.isEmpty()) {
            return destinationTable;
        }
        // Extract table name from schema.table format
        int dot = table.indexOf('.');
        return dot >= 0 ? table.substring(dot + 1) : table;
    }

    // Getters and setters
    public String getTable() { return table; }
    public void setTable(String table) { this.table = table; }
    public String getSourceTopic() { return sourceTopic; }
    public void setSourceTopic(String sourceTopic) { this.sourceTopic = sourceTopic; }
    public String getSyncMode() { return syncMode; }
    public void setSyncMode(String syncMode) { this.syncMode = syncMode; }
    public boolean isNormalizePayload() { return normalizePayload; }
    public void setNormalizePayload(boolean normalizePayload) { this.normalizePayload = normalizePayload; }
    public List<PayloadColumnConfig> getPayloadColumns() { return payloadColumns; }
    public void setPayloadColumns(List<PayloadColumnConfig> payloadColumns) { this.payloadColumns = payloadColumns; }
    public String getCursorField() { return cursorField; }
    public void setCursorField(String cursorField) { this.cursorField = cursorField; }
    public List<String> getPrimaryKey() { return primaryKey; }
    public void setPrimaryKey(List<String> primaryKey) { this.primaryKey = primaryKey; }
    public String getDestinationTable() { return destinationTable; }
    public void setDestinationTable(String destinationTable) { this.destinationTable = destinationTable; }
}
