package com.cdc.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DestinationConfig {
    private String type = "parquet";

    @JsonProperty("base_path")
    private String basePath = "/data/output";

    @JsonProperty("file_size_mb")
    private int fileSizeMb = 256;

    private String compression = "SNAPPY";

    @JsonProperty("row_group_size_mb")
    private int rowGroupSizeMb = 128;

    // Iceberg-specific
    private String warehouse;

    @JsonProperty("catalog_type")
    private String catalogType = "hadoop";

    @JsonProperty("catalog_properties")
    private Map<String, String> catalogProperties = Map.of();

    @JsonProperty("table_namespace")
    private String tableNamespace = "cdc_db";

    public boolean isIceberg() {
        return "iceberg".equalsIgnoreCase(type);
    }

    public boolean isParquet() {
        return "parquet".equalsIgnoreCase(type);
    }

    // Getters and setters
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getBasePath() { return basePath; }
    public void setBasePath(String basePath) { this.basePath = basePath; }
    public int getFileSizeMb() { return fileSizeMb; }
    public void setFileSizeMb(int fileSizeMb) { this.fileSizeMb = fileSizeMb; }
    public String getCompression() { return compression; }
    public void setCompression(String compression) { this.compression = compression; }
    public int getRowGroupSizeMb() { return rowGroupSizeMb; }
    public void setRowGroupSizeMb(int rowGroupSizeMb) { this.rowGroupSizeMb = rowGroupSizeMb; }
    public String getWarehouse() { return warehouse; }
    public void setWarehouse(String warehouse) { this.warehouse = warehouse; }
    public String getCatalogType() { return catalogType; }
    public void setCatalogType(String catalogType) { this.catalogType = catalogType; }
    public Map<String, String> getCatalogProperties() { return catalogProperties; }
    public void setCatalogProperties(Map<String, String> catalogProperties) { this.catalogProperties = catalogProperties; }
    public String getTableNamespace() { return tableNamespace; }
    public void setTableNamespace(String tableNamespace) { this.tableNamespace = tableNamespace; }
}
