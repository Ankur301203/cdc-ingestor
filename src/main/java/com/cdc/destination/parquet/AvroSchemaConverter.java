package com.cdc.destination.parquet;

import com.cdc.protocol.schema.CdcSchema;
import com.cdc.protocol.schema.CdcType;
import com.cdc.protocol.schema.Column;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.ArrayList;
import java.util.List;

public class AvroSchemaConverter {

    public Schema convert(CdcSchema cdcSchema) {
        List<Schema.Field> fields = new ArrayList<>();

        for (Column column : cdcSchema.columns()) {
            Schema fieldSchema = convertType(column.type());
            if (column.nullable()) {
                fieldSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), fieldSchema);
            }
            fields.add(new Schema.Field(column.name(), fieldSchema, null, (Object) null));
        }

        // Add CDC metadata fields for change tracking
        Schema nullableString = Schema.createUnion(
            Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
        fields.add(new Schema.Field("_cdc_operation", nullableString, null, (Object) null));

        Schema timestampSchema = LogicalTypes.timestampMicros().addToSchema(
            Schema.create(Schema.Type.LONG));
        Schema nullableTimestamp = Schema.createUnion(
            Schema.create(Schema.Type.NULL), timestampSchema);
        fields.add(new Schema.Field("_cdc_timestamp", nullableTimestamp, null, (Object) null));

        return Schema.createRecord(
            cdcSchema.tableName(),
            null,
            cdcSchema.schemaName(),
            false,
            fields
        );
    }

    private Schema convertType(CdcType type) {
        return switch (type) {
            case INT32 -> Schema.create(Schema.Type.INT);
            case INT64 -> Schema.create(Schema.Type.LONG);
            case FLOAT -> Schema.create(Schema.Type.FLOAT);
            case DOUBLE -> Schema.create(Schema.Type.DOUBLE);
            case BOOLEAN -> Schema.create(Schema.Type.BOOLEAN);
            case BYTES -> Schema.create(Schema.Type.BYTES);
            case STRING, JSON, UUID -> Schema.create(Schema.Type.STRING);
            case DATE -> LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
            case TIME -> LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
            case TIMESTAMP, TIMESTAMP_TZ -> LogicalTypes.timestampMicros().addToSchema(
                Schema.create(Schema.Type.LONG));
            case DECIMAL -> LogicalTypes.decimal(38, 18).addToSchema(
                Schema.create(Schema.Type.BYTES));
            case ARRAY -> Schema.createArray(Schema.create(Schema.Type.STRING));
        };
    }
}
