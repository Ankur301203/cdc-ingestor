package com.cdc.config;

import com.cdc.protocol.schema.CdcType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PayloadColumnConfig {
    private String name;
    private CdcType type = CdcType.STRING;
    private boolean nullable = true;

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public CdcType getType() { return type; }
    public void setType(CdcType type) { this.type = type; }
    public boolean isNullable() { return nullable; }
    public void setNullable(boolean nullable) { this.nullable = nullable; }
}

