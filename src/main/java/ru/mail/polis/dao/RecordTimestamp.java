package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Comparator;

public class RecordTimestamp {
    private final long timestamp;
    private final ByteBuffer value;
    private final TypeOfRecord type;

    public RecordTimestamp(final ByteBuffer value, final long timestamp, final TypeOfRecord type) {
        this.value = value;
        this.timestamp = timestamp;
        this.type = type;
    }

    public enum TypeOfRecord {
        VALUE((byte) 1),
        DELETED((byte) -1),
        MISSING((byte) 0);

        final byte value;

        TypeOfRecord(final byte value) {
            this.value = value;
        }

        static TypeOfRecord fromValue(final byte value) {
            if (value == VALUE.value) {
                return VALUE;
            } else if (value == DELETED.value) {
                return DELETED;
            } else {
                return  MISSING;
            }
        }
    }

    public boolean isValue(){
        return type == TypeOfRecord.VALUE;
    }

    public boolean isDeleted(){
        return type == TypeOfRecord.DELETED;
    }

    public boolean isMissing(){
        return type == TypeOfRecord.MISSING;
    }



    public byte[] toBytes() {
        final var valSize = isValue() ? value.remaining() : 0;
        final ByteBuffer buff =  ByteBuffer.allocate(1 + Long.BYTES + valSize);
        buff.put(type.value);
        buff.putLong(getTimestamp());
        if (isValue()) {
            buff.put(value.duplicate());
        }
        return buff.array();
    }

    public static RecordTimestamp fromBytes(final byte[] bytes) {
        if (bytes == null)
                return new RecordTimestamp(null, -1, TypeOfRecord.MISSING);
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final TypeOfRecord type = TypeOfRecord.fromValue(buffer.get());
        final long timestamp = buffer.getLong();
        return new RecordTimestamp(buffer, timestamp, type);
    }

    public ByteBuffer getValue() {
        if (!isValue()) {
            throw new IllegalStateException("no value in record");
        }
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public static RecordTimestamp getEmptyRecord() {
        return new RecordTimestamp(null, -1, TypeOfRecord.MISSING);
    }

    public static RecordTimestamp mergeRecords(final List<RecordTimestamp> responses) {
        if (responses.size() == 1) return responses.get(0);
        else {
            return responses.stream()
                    .filter(timestampRecord -> !timestampRecord.isMissing())
                    .max(Comparator.comparingLong(RecordTimestamp::getTimestamp))
                    .orElseGet(RecordTimestamp::getEmptyRecord);
        }
    }

    public byte[] getValueInByteFormat() {
        final var value = getValue().duplicate();
        final byte[] bytes = new byte[value.remaining()];
        value.get(bytes);
        return bytes;
    }

    public static RecordTimestamp fromValue(@NotNull final ByteBuffer value, final long timestamp) {
        return new  RecordTimestamp(value, timestamp,TypeOfRecord.VALUE);
    }

    public static RecordTimestamp tombstone(final long timestamp) {
        return new RecordTimestamp( null,timestamp, TypeOfRecord.DELETED);
    }
}
