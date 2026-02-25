package com.cdc.source.postgres;

import com.cdc.protocol.OperationType;
import com.cdc.protocol.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

public class PgOutputDecoder {
    private static final Logger log = LoggerFactory.getLogger(PgOutputDecoder.class);

    // Relation metadata cached from 'R' messages
    private final Map<Integer, RelationInfo> relations = new HashMap<>();

    public record RelationInfo(
        int oid,
        String namespace,
        String tableName,
        List<ColumnInfo> columns
    ) {
        public String fullTableName() {
            return namespace + "." + tableName;
        }
    }

    public record ColumnInfo(
        String name,
        int typeOid
    ) {}

    public record DecodedMessage(
        MessageType type,
        Record record,
        long commitLsn
    ) {}

    public enum MessageType {
        BEGIN, COMMIT, RELATION, INSERT, UPDATE, DELETE, TRUNCATE, UNKNOWN
    }

    public DecodedMessage decode(ByteBuffer buffer) {
        if (!buffer.hasRemaining()) return null;

        byte msgType = buffer.get();
        return switch ((char) msgType) {
            case 'B' -> decodeBegin(buffer);
            case 'C' -> decodeCommit(buffer);
            case 'R' -> decodeRelation(buffer);
            case 'I' -> decodeInsert(buffer);
            case 'U' -> decodeUpdate(buffer);
            case 'D' -> decodeDelete(buffer);
            case 'T' -> decodeTruncate(buffer);
            default -> {
                log.trace("Unknown message type: {}", (char) msgType);
                yield new DecodedMessage(MessageType.UNKNOWN, null, 0);
            }
        };
    }

    private DecodedMessage decodeBegin(ByteBuffer buf) {
        long finalLsn = buf.getLong();   // final LSN of the transaction
        long timestamp = buf.getLong();  // commit timestamp
        int xid = buf.getInt();          // transaction ID
        log.trace("BEGIN xid={} lsn={}", xid, finalLsn);
        return new DecodedMessage(MessageType.BEGIN, null, finalLsn);
    }

    private DecodedMessage decodeCommit(ByteBuffer buf) {
        byte flags = buf.get();
        long commitLsn = buf.getLong();
        long endLsn = buf.getLong();
        long timestamp = buf.getLong();
        log.trace("COMMIT lsn={}", endLsn);
        return new DecodedMessage(MessageType.COMMIT, null, endLsn);
    }

    private DecodedMessage decodeRelation(ByteBuffer buf) {
        int oid = buf.getInt();
        String namespace = readString(buf);
        String tableName = readString(buf);
        byte replicaIdentity = buf.get();
        short numColumns = buf.getShort();

        List<ColumnInfo> columns = new ArrayList<>();
        for (int i = 0; i < numColumns; i++) {
            byte flags = buf.get();      // column flags
            String colName = readString(buf);
            int typeOid = buf.getInt();
            int typeMod = buf.getInt();
            columns.add(new ColumnInfo(colName, typeOid));
        }

        RelationInfo info = new RelationInfo(oid, namespace, tableName, columns);
        relations.put(oid, info);
        log.debug("RELATION {}.{} (oid={}, {} columns)", namespace, tableName, oid, numColumns);
        return new DecodedMessage(MessageType.RELATION, null, 0);
    }

    private DecodedMessage decodeInsert(ByteBuffer buf) {
        int oid = buf.getInt();
        byte tupleFlag = buf.get(); // 'N' for new tuple
        Map<String, Object> data = decodeTuple(buf, oid);

        RelationInfo rel = relations.get(oid);
        String tableName = rel != null ? rel.fullTableName() : "unknown." + oid;

        Record record = new Record(tableName, OperationType.INSERT, data, null, 0, Instant.now());
        return new DecodedMessage(MessageType.INSERT, record, 0);
    }

    private DecodedMessage decodeUpdate(ByteBuffer buf) {
        int oid = buf.getInt();
        Map<String, Object> beforeData = null;

        byte flag = buf.get();
        // 'K' = old tuple with key columns, 'O' = old tuple with all columns
        if (flag == 'K' || flag == 'O') {
            beforeData = decodeTuple(buf, oid);
            flag = buf.get(); // should be 'N' for new tuple
        }

        Map<String, Object> newData = decodeTuple(buf, oid);
        RelationInfo rel = relations.get(oid);
        String tableName = rel != null ? rel.fullTableName() : "unknown." + oid;

        Record record = new Record(tableName, OperationType.UPDATE, newData, beforeData, 0, Instant.now());
        return new DecodedMessage(MessageType.UPDATE, record, 0);
    }

    private DecodedMessage decodeDelete(ByteBuffer buf) {
        int oid = buf.getInt();
        byte flag = buf.get(); // 'K' or 'O'
        Map<String, Object> oldData = decodeTuple(buf, oid);

        RelationInfo rel = relations.get(oid);
        String tableName = rel != null ? rel.fullTableName() : "unknown." + oid;

        Record record = new Record(tableName, OperationType.DELETE, null, oldData, 0, Instant.now());
        return new DecodedMessage(MessageType.DELETE, record, 0);
    }

    private DecodedMessage decodeTruncate(ByteBuffer buf) {
        int numRelations = buf.getInt();
        byte options = buf.get();
        for (int i = 0; i < numRelations; i++) {
            buf.getInt(); // relation OID
        }
        return new DecodedMessage(MessageType.TRUNCATE, null, 0);
    }

    private Map<String, Object> decodeTuple(ByteBuffer buf, int relationOid) {
        short numColumns = buf.getShort();
        RelationInfo rel = relations.get(relationOid);
        Map<String, Object> data = new LinkedHashMap<>();

        for (int i = 0; i < numColumns; i++) {
            byte colType = buf.get();
            String colName = (rel != null && i < rel.columns().size())
                ? rel.columns().get(i).name() : "col_" + i;

            switch (colType) {
                case 'n' -> data.put(colName, null);             // null
                case 'u' -> data.put(colName, null);             // unchanged TOAST
                case 't' -> {                                     // text value
                    int len = buf.getInt();
                    byte[] bytes = new byte[len];
                    buf.get(bytes);
                    data.put(colName, new String(bytes, StandardCharsets.UTF_8));
                }
                default -> log.warn("Unknown column type byte: {}", colType);
            }
        }
        return data;
    }

    private String readString(ByteBuffer buf) {
        StringBuilder sb = new StringBuilder();
        byte b;
        while ((b = buf.get()) != 0) {
            sb.append((char) b);
        }
        return sb.toString();
    }

    public Map<Integer, RelationInfo> getRelations() {
        return Collections.unmodifiableMap(relations);
    }
}
