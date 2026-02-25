package com.cdc.protocol.schema;

public record Column(
    String name,
    CdcType type,
    boolean nullable,
    boolean primaryKey
) {}
