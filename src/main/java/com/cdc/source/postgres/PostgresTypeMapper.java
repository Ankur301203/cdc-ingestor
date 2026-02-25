package com.cdc.source.postgres;

import com.cdc.protocol.schema.CdcType;

import java.util.Map;

public class PostgresTypeMapper {
    private static final Map<String, CdcType> TYPE_MAP = Map.ofEntries(
        Map.entry("smallint", CdcType.INT32),
        Map.entry("int2", CdcType.INT32),
        Map.entry("integer", CdcType.INT32),
        Map.entry("int4", CdcType.INT32),
        Map.entry("bigint", CdcType.INT64),
        Map.entry("int8", CdcType.INT64),
        Map.entry("serial", CdcType.INT32),
        Map.entry("bigserial", CdcType.INT64),
        Map.entry("real", CdcType.FLOAT),
        Map.entry("float4", CdcType.FLOAT),
        Map.entry("double precision", CdcType.DOUBLE),
        Map.entry("float8", CdcType.DOUBLE),
        Map.entry("boolean", CdcType.BOOLEAN),
        Map.entry("bool", CdcType.BOOLEAN),
        Map.entry("text", CdcType.STRING),
        Map.entry("varchar", CdcType.STRING),
        Map.entry("character varying", CdcType.STRING),
        Map.entry("char", CdcType.STRING),
        Map.entry("character", CdcType.STRING),
        Map.entry("name", CdcType.STRING),
        Map.entry("bytea", CdcType.BYTES),
        Map.entry("date", CdcType.DATE),
        Map.entry("time", CdcType.TIME),
        Map.entry("time without time zone", CdcType.TIME),
        Map.entry("time with time zone", CdcType.TIME),
        Map.entry("timetz", CdcType.TIME),
        Map.entry("timestamp", CdcType.TIMESTAMP),
        Map.entry("timestamp without time zone", CdcType.TIMESTAMP),
        Map.entry("timestamp with time zone", CdcType.TIMESTAMP_TZ),
        Map.entry("timestamptz", CdcType.TIMESTAMP_TZ),
        Map.entry("numeric", CdcType.DECIMAL),
        Map.entry("decimal", CdcType.DECIMAL),
        Map.entry("json", CdcType.JSON),
        Map.entry("jsonb", CdcType.JSON),
        Map.entry("uuid", CdcType.UUID)
    );

    public static CdcType map(String pgType) {
        if (pgType == null) return CdcType.STRING;
        String normalized = pgType.toLowerCase().trim();

        // Check for array types
        if (normalized.startsWith("_") || normalized.endsWith("[]")) {
            return CdcType.ARRAY;
        }

        CdcType type = TYPE_MAP.get(normalized);
        if (type != null) return type;

        // Fallback: check if type name contains known keywords
        if (normalized.contains("int")) return CdcType.INT64;
        if (normalized.contains("float") || normalized.contains("double")) return CdcType.DOUBLE;
        if (normalized.contains("bool")) return CdcType.BOOLEAN;
        if (normalized.contains("timestamp")) return CdcType.TIMESTAMP;
        if (normalized.contains("date")) return CdcType.DATE;
        if (normalized.contains("time")) return CdcType.TIME;
        if (normalized.contains("numeric") || normalized.contains("decimal")) return CdcType.DECIMAL;

        return CdcType.STRING;
    }
}
