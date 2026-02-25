package com.cdc.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CatalogConfig {
    private List<StreamEntry> streams = List.of();

    public List<StreamEntry> getStreams() { return streams; }
    public void setStreams(List<StreamEntry> streams) { this.streams = streams; }
}
