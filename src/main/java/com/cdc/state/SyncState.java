package com.cdc.state;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SyncState {
    private Map<String, StreamState> streams = new HashMap<>();

    @JsonProperty("global_lsn")
    private String globalLsn;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class StreamState {
        @JsonProperty("sync_mode")
        private String syncMode;

        @JsonProperty("last_lsn")
        private String lastLsn;

        @JsonProperty("full_load_completed")
        private boolean fullLoadCompleted;

        @JsonProperty("full_load_chunks_done")
        private int fullLoadChunksDone;

        @JsonProperty("full_load_chunks_total")
        private int fullLoadChunksTotal;

        @JsonProperty("last_sync_time")
        private Instant lastSyncTime;

        public String getSyncMode() { return syncMode; }
        public void setSyncMode(String syncMode) { this.syncMode = syncMode; }
        public String getLastLsn() { return lastLsn; }
        public void setLastLsn(String lastLsn) { this.lastLsn = lastLsn; }
        public boolean isFullLoadCompleted() { return fullLoadCompleted; }
        public void setFullLoadCompleted(boolean fullLoadCompleted) { this.fullLoadCompleted = fullLoadCompleted; }
        public int getFullLoadChunksDone() { return fullLoadChunksDone; }
        public void setFullLoadChunksDone(int fullLoadChunksDone) { this.fullLoadChunksDone = fullLoadChunksDone; }
        public int getFullLoadChunksTotal() { return fullLoadChunksTotal; }
        public void setFullLoadChunksTotal(int fullLoadChunksTotal) { this.fullLoadChunksTotal = fullLoadChunksTotal; }
        public Instant getLastSyncTime() { return lastSyncTime; }
        public void setLastSyncTime(Instant lastSyncTime) { this.lastSyncTime = lastSyncTime; }
    }

    public StreamState getOrCreateStream(String tableName) {
        return streams.computeIfAbsent(tableName, k -> new StreamState());
    }

    public Map<String, StreamState> getStreams() { return streams; }
    public void setStreams(Map<String, StreamState> streams) { this.streams = streams; }
    public String getGlobalLsn() { return globalLsn; }
    public void setGlobalLsn(String globalLsn) { this.globalLsn = globalLsn; }
}
