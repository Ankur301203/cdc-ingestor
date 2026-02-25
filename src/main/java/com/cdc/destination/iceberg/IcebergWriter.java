package com.cdc.destination.iceberg;

import com.cdc.config.DestinationConfig;
import com.cdc.protocol.Record;
import com.cdc.protocol.RecordBatch;
import com.cdc.protocol.Writer;
import com.cdc.protocol.schema.CdcSchema;
import com.cdc.protocol.schema.CdcType;
import com.cdc.protocol.schema.Column;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class IcebergWriter implements Writer {
    private static final Logger log = LoggerFactory.getLogger(IcebergWriter.class);
    private static final int BUFFER_SIZE = 10000;

    private CdcSchema cdcSchema;
    private DestinationConfig config;
    private Catalog catalog;
    private Table table;
    private Schema icebergSchema;
    private DataWriter<GenericRecord> dataWriter;
    private final List<DataFile> dataFiles = new ArrayList<>();
    private final List<GenericRecord> buffer = new ArrayList<>();
    private long recordsWritten;
    private int fileSeq;

    @Override
    public void open(CdcSchema schema, DestinationConfig config) {
        this.cdcSchema = schema;
        this.config = config;
        this.recordsWritten = 0;
        this.fileSeq = 0;

        // Convert schema
        IcebergSchemaConverter converter = new IcebergSchemaConverter();
        this.icebergSchema = converter.convert(schema);

        // Create catalog and ensure table exists
        this.catalog = IcebergCatalogFactory.create(config);
        TableIdentifier tableId = TableIdentifier.of(
            Namespace.of(config.getTableNamespace()), schema.tableName());

        if (catalog instanceof org.apache.iceberg.hadoop.HadoopCatalog hadoopCatalog) {
            if (!hadoopCatalog.tableExists(tableId)) {
                log.info("Creating Iceberg table: {}", tableId);
                this.table = hadoopCatalog.createTable(tableId, icebergSchema);
            } else {
                this.table = hadoopCatalog.loadTable(tableId);
            }
        }

        openNewDataWriter();
        log.info("Iceberg writer opened for table {}", tableId);
    }

    @Override
    public void writeRecord(Record record) {
        GenericRecord icebergRecord = convertToIceberg(record);
        buffer.add(icebergRecord);

        if (buffer.size() >= BUFFER_SIZE) {
            flushBuffer();
        }
    }

    @Override
    public void writeBatch(RecordBatch batch) {
        for (Record record : batch.records()) {
            writeRecord(record);
        }
    }

    @Override
    public void flush() {
        flushBuffer();
        commitDataWriter();
        commitTransaction();
    }

    @Override
    public void close() {
        flushBuffer();
        commitDataWriter();
        commitTransaction();
        log.info("Iceberg writer closed. Total records: {}", recordsWritten);
    }

    private void flushBuffer() {
        if (buffer.isEmpty()) return;
        for (GenericRecord record : buffer) {
            dataWriter.write(record);
            recordsWritten++;
        }
        buffer.clear();
    }

    private void commitDataWriter() {
        if (dataWriter == null) return;
        try {
            dataWriter.close();
            dataFiles.add(dataWriter.toDataFile());
            openNewDataWriter();
        } catch (IOException e) {
            throw new RuntimeException("Failed to close Iceberg data writer", e);
        }
    }

    private void commitTransaction() {
        if (dataFiles.isEmpty()) return;

        AppendFiles append = table.newAppend();
        for (DataFile file : dataFiles) {
            append.appendFile(file);
        }
        append.commit();
        log.info("Committed {} data files to Iceberg table", dataFiles.size());
        dataFiles.clear();
    }

    private void openNewDataWriter() {
        try {
            fileSeq++;
            String filename = String.format("data-%05d-%s.parquet", fileSeq, UUID.randomUUID());
            OutputFile outputFile = table.io().newOutputFile(
                table.location() + "/data/" + filename);

            dataWriter = Parquet.writeData(outputFile)
                .schema(icebergSchema)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .overwrite()
                .build();
        } catch (IOException e) {
            throw new RuntimeException("Failed to open Iceberg data writer", e);
        }
    }

    private GenericRecord convertToIceberg(Record record) {
        GenericRecord icebergRecord = GenericRecord.create(icebergSchema);
        Map<String, Object> data = record.data() != null ? record.data() : record.beforeData();

        if (data != null) {
            for (Column column : cdcSchema.columns()) {
                Object value = data.get(column.name());
                icebergRecord.setField(column.name(), convertValue(value, column.type()));
            }
        }

        icebergRecord.setField("_cdc_operation", record.operation().name());
        if (record.timestamp() != null) {
            icebergRecord.setField("_cdc_timestamp",
                record.timestamp().atOffset(ZoneOffset.UTC));
        }

        return icebergRecord;
    }

    private Object convertValue(Object value, CdcType type) {
        if (value == null) return null;
        String strValue = value.toString();

        return switch (type) {
            case INT32 -> value instanceof Number n ? n.intValue() : Integer.parseInt(strValue);
            case INT64 -> value instanceof Number n ? n.longValue() : Long.parseLong(strValue);
            case FLOAT -> value instanceof Number n ? n.floatValue() : Float.parseFloat(strValue);
            case DOUBLE -> value instanceof Number n ? n.doubleValue() : Double.parseDouble(strValue);
            case BOOLEAN -> value instanceof Boolean b ? b : Boolean.parseBoolean(strValue);
            case STRING, JSON, UUID -> strValue;
            case BYTES -> value instanceof byte[] bytes ? java.nio.ByteBuffer.wrap(bytes)
                : java.nio.ByteBuffer.wrap(strValue.getBytes());
            case DATE -> value instanceof java.sql.Date d ? d.toLocalDate()
                : LocalDate.parse(strValue);
            case TIME -> value instanceof java.sql.Time t ? t.toLocalTime()
                : LocalTime.parse(strValue);
            case TIMESTAMP -> value instanceof java.sql.Timestamp ts ? ts.toLocalDateTime()
                : LocalDateTime.parse(strValue);
            case TIMESTAMP_TZ -> value instanceof java.sql.Timestamp ts
                ? ts.toInstant().atOffset(ZoneOffset.UTC)
                : OffsetDateTime.parse(strValue);
            case DECIMAL -> value instanceof BigDecimal bd ? bd : new BigDecimal(strValue);
            case ARRAY -> strValue;
        };
    }
}
