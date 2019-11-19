package ru.mail.polis.dao;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksDBException;
import org.rocksdb.Options;
import org.rocksdb.CompressionType;
import org.rocksdb.BuiltinComparator;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DAOImpl implements DAO {
    private final RocksDB db;
    private final AtomicBoolean open = new AtomicBoolean();

    private DAOImpl(@NotNull final RocksDB db) {
        this.db = db;
        this.open.set(true);
    }

    static DAO init(final File data) throws IOException {
        RocksDB.loadLibrary();
        try {
            final var options = new Options()
                    .setCreateIfMissing(true)
                    .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR)
                    .setCompressionType(CompressionType.NO_COMPRESSION)
                    .setMaxBackgroundCompactions(2)
                    .setMaxBackgroundFlushes(2);
            final var db = RocksDB.open(options, data.getAbsolutePath());
            return new DAOImpl(db);
        } catch (RocksDBException e) {
            throw new IOException("can't create DAO", e);
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final RocksIterator Iterator = db.newIterator();
        final byte[] unpackedKey = ByteBufferUtils.shift(from);
        Iterator.seek(unpackedKey);
        return new RocksDBIterator(Iterator);
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws IOException,NoSuchElementException {
            try {
                final byte[] unpackedKey = ByteBufferUtils.shift(key);
                final byte[] valueByteArray = db.get(unpackedKey);
                if (valueByteArray == null) {
                    throw new NoSuchElementException("Key is not present!");
                }
                return ByteBuffer.wrap(valueByteArray);
            } catch (RocksDBException e) {
                throw new IOException("can't get", e);
            }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
            try {
                final byte[] unpackedKey = ByteBufferUtils.shift(key);
                final byte[] arrayValue = ByteBufferUtils.fromByteToArray(value);
                db.put(unpackedKey, arrayValue);
            } catch (RocksDBException e) {
                throw new IOException("can't upsert", e);
            }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        try {
            final byte[] unpackedKey = ByteBufferUtils.shift(key);
            db.delete(unpackedKey);
        } catch (RocksDBException e) {
            throw new IOException("can't remove", e);
        }
    }

    /**
     * Get record with timestamp.
     *
     * @param key  key
     */
    @NotNull
    public RecordTimestamp getWithTimestamp(@NotNull final ByteBuffer key) throws IOException, NoSuchElementException {
        try {
            final byte[] unpackedKey = ByteBufferUtils.shift(key);
            final byte[] valueByteArray = db.get(unpackedKey);
            return RecordTimestamp.fromBytes(valueByteArray);
        } catch (RocksDBException e) {
            throw new IOException("can't get /timestamp", e);
        }
    }

    /**
     * Put record with timestamp.
     *
     * @param key key
     * @param value  value
     */
    public void upsertWithTimestamp(@NotNull final ByteBuffer key,
                                    @NotNull final ByteBuffer value) throws IOException {
        try {
            final var record = RecordTimestamp.fromValue(value, System.currentTimeMillis());
            final byte[] unpackedKey = ByteBufferUtils.shift(key);
            final byte[] valueByteArray = record.toBytes();
            db.put(unpackedKey, valueByteArray);
        } catch (RocksDBException e) {
            throw new IOException("can't upsert /timestamp", e);
        }
    }

    /**
     * Delete record with timestamp (put tombstone record).
     *
     * @param key key
     */
    public void removeWithTimestamp(@NotNull final ByteBuffer key) throws IOException {
       try {
            final var record = RecordTimestamp.tombstone(System.currentTimeMillis());
            final byte[] unpackedKey = ByteBufferUtils.shift(key);
            final byte[] valueByteArray = record.toBytes();
            db.put(unpackedKey, valueByteArray);
        } catch (RocksDBException e) {
            throw new IOException("can't remove /timestamp", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            db.syncWal();
            db.close();
        } catch (RocksDBException e) {
            throw new IOException("Error while close", e);
        }
    }

    @Override
    public void compact() throws IOException {
        try {
            db.compactRange();
        } catch (RocksDBException e) {
            throw new IOException("can't compact", e);
        }
    }
}
