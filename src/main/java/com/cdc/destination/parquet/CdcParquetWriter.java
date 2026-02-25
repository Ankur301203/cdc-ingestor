package com.cdc.destination.parquet;

import com.cdc.config.DestinationConfig;
import com.cdc.protocol.Record;
import com.cdc.protocol.RecordBatch;
import com.cdc.protocol.Writer;
import com.cdc.protocol.schema.CdcSchema;
import com.cdc.protocol.schema.CdcType;
import com.cdc.protocol.schema.Column;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CdcParquetWriter implements Writer {
    private static final Logger log = LoggerFactory.getLogger(CdcParquetWriter.class);

    private CdcSchema cdcSchema;
    private Schema avroSchema;
    private ParquetWriter<GenericRecord> parquetWriter;
    private ParquetFileManager fileManager;
    private java.nio.file.Path currentFilePath;
    private DestinationConfig config;
    private long recordsWritten;
    private final List<GenericRecord> buffer = new ArrayList<>();
    private static final int BUFFER_SIZE = 10000;

    @Override
    public void open(CdcSchema schema, DestinationConfig config) {
        this.cdcSchema = schema;
        this.config = config;

        AvroSchemaConverter converter = new AvroSchemaConverter();
        this.avroSchema = converter.convert(schema);
        this.fileManager = new ParquetFileManager(config.getBasePath(), config.getFileSizeMb());
        this.recordsWritten = 0;

        try {
            rotateFile();
        } catch (IOException e) {
            throw new RuntimeException("Failed to open parquet writer", e);
        }

        log.info("Parquet writer opened for table {} at {}", schema.fullTableName(), config.getBasePath());
    }

    @Override
    public void writeRecord(Record record) {
        GenericRecord avroRecord = convertToAvro(record);
        buffer.add(avroRecord);

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
        try {
            if (parquetWriter != null) {
                parquetWriter.close();
                log.info("Flushed parquet file: {} ({} records total)",
                    currentFilePath, recordsWritten);
                rotateFile();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush parquet writer", e);
        }
    }

    @Override
    public void close() {
        flushBuffer();
        try {
            if (parquetWriter != null) {
                parquetWriter.close();
                log.info("Closed parquet writer. Total records: {}", recordsWritten);
            }
        } catch (IOException e) {
            log.error("Error closing parquet writer", e);
        }
    }

    private void flushBuffer() {
        if (buffer.isEmpty()) return;
        try {
            for (GenericRecord record : buffer) {
                parquetWriter.write(record);
                recordsWritten++;
            }
            buffer.clear();

            // Check if file rotation is needed
            if (fileManager.shouldRotate(currentFilePath)) {
                parquetWriter.close();
                rotateFile();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write records to parquet", e);
        }
    }

    private void rotateFile() throws IOException {
        currentFilePath = fileManager.getNextFilePath(cdcSchema.tableName());

        CompressionCodecName codec = CompressionCodecName.fromConf(
            config.getCompression() != null ? config.getCompression() : "SNAPPY");

        Configuration hadoopConf = new Configuration();
        parquetWriter = AvroParquetWriter.<GenericRecord>builder(
                new Path(currentFilePath.toString()))
            .withSchema(avroSchema)
            .withConf(hadoopConf)
            .withCompressionCodec(codec)
            .withRowGroupSize((long) config.getRowGroupSizeMb() * 1024 * 1024)
            .build();

        log.debug("Rotated to new parquet file: {}", currentFilePath);
    }

    private GenericRecord convertToAvro(Record record) {
        GenericRecord avroRecord = new GenericData.Record(avroSchema);
        Map<String, Object> data = record.data() != null ? record.data() : record.beforeData();

        if (data != null) {
            for (Column column : cdcSchema.columns()) {
                Object value = data.get(column.name());
                avroRecord.put(column.name(), convertValue(value, column.type()));
            }
        }

        // Set CDC metadata
        avroRecord.put("_cdc_operation", record.operation().name());
        if (record.timestamp() != null) {
            avroRecord.put("_cdc_timestamp", record.timestamp().toEpochMilli() * 1000); // micros
        }

        return avroRecord;
    }

    private Object convertValue(Object value, CdcType type) {
        if (value == null) return null;

        // If value comes as String from pgoutput, parse accordingly
        String strValue = value.toString();

        return switch (type) {
            case INT32 -> {
                if (value instanceof Number n) yield n.intValue();
                yield Integer.parseInt(strValue);
            }
            case INT64 -> {
                if (value instanceof Number n) yield n.longValue();
                yield Long.parseLong(strValue);
            }
            case FLOAT -> {
                if (value instanceof Number n) yield n.floatValue();
                yield Float.parseFloat(strValue);
            }
            case DOUBLE -> {
                if (value instanceof Number n) yield n.doubleValue();
                yield Double.parseDouble(strValue);
            }
            case BOOLEAN -> {
                if (value instanceof Boolean b) yield b;
                yield Boolean.parseBoolean(strValue);
            }
            case STRING, JSON, UUID -> strValue;
            case BYTES -> {
                if (value instanceof byte[] bytes) yield ByteBuffer.wrap(bytes);
                yield ByteBuffer.wrap(strValue.getBytes());
            }
            case DATE -> {
                if (value instanceof java.sql.Date d) yield (int) d.toLocalDate().toEpochDay();
                if (value instanceof LocalDate d) yield (int) d.toEpochDay();
                yield (int) LocalDate.parse(strValue).toEpochDay();
            }
            case TIME -> {
                if (value instanceof java.sql.Time t) yield TimeUnit.MILLISECONDS.toMicros(t.getTime());
                yield TimeUnit.MILLISECONDS.toMicros(
                    java.sql.Time.valueOf(strValue).getTime());
            }
            case TIMESTAMP -> {
                if (value instanceof java.sql.Timestamp ts)
                    yield TimeUnit.MILLISECONDS.toMicros(ts.getTime());
                yield TimeUnit.MILLISECONDS.toMicros(
                    java.sql.Timestamp.valueOf(strValue).getTime());
            }
            case TIMESTAMP_TZ -> {
                if (value instanceof java.sql.Timestamp ts)
                    yield TimeUnit.MILLISECONDS.toMicros(ts.getTime());
                if (value instanceof OffsetDateTime odt)
                    yield TimeUnit.MILLISECONDS.toMicros(odt.toInstant().toEpochMilli());
                yield TimeUnit.MILLISECONDS.toMicros(
                    java.sql.Timestamp.valueOf(strValue).getTime());
            }
            case DECIMAL -> {
                if (value instanceof BigDecimal bd)
                    yield ByteBuffer.wrap(bd.unscaledValue().toByteArray());
                yield ByteBuffer.wrap(new BigDecimal(strValue).unscaledValue().toByteArray());
            }
            case ARRAY -> strValue;
        };
    }
}
