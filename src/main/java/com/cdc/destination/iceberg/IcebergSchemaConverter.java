package com.cdc.destination.iceberg;

import com.cdc.protocol.schema.CdcSchema;
import com.cdc.protocol.schema.CdcType;
import com.cdc.protocol.schema.Column;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class IcebergSchemaConverter {

    public Schema convert(CdcSchema cdcSchema) {
        AtomicInteger fieldId = new AtomicInteger(0);
        List<Types.NestedField> fields = new ArrayList<>();

        for (Column column : cdcSchema.columns()) {
            Type icebergType = convertType(column.type());
            int id = fieldId.incrementAndGet();

            if (column.nullable()) {
                fields.add(Types.NestedField.optional(id, column.name(), icebergType));
            } else {
                fields.add(Types.NestedField.required(id, column.name(), icebergType));
            }
        }

        // Add CDC metadata fields
        fields.add(Types.NestedField.optional(
            fieldId.incrementAndGet(), "_cdc_operation", Types.StringType.get()));
        fields.add(Types.NestedField.optional(
            fieldId.incrementAndGet(), "_cdc_timestamp", Types.TimestampType.withZone()));

        return new Schema(fields);
    }

    private Type convertType(CdcType type) {
        return switch (type) {
            case INT32 -> Types.IntegerType.get();
            case INT64 -> Types.LongType.get();
            case FLOAT -> Types.FloatType.get();
            case DOUBLE -> Types.DoubleType.get();
            case BOOLEAN -> Types.BooleanType.get();
            case BYTES -> Types.BinaryType.get();
            case STRING, JSON, UUID -> Types.StringType.get();
            case DATE -> Types.DateType.get();
            case TIME -> Types.TimeType.get();
            case TIMESTAMP -> Types.TimestampType.withoutZone();
            case TIMESTAMP_TZ -> Types.TimestampType.withZone();
            case DECIMAL -> Types.DecimalType.of(38, 18);
            case ARRAY -> Types.ListType.ofOptional(999, Types.StringType.get());
        };
    }
}
