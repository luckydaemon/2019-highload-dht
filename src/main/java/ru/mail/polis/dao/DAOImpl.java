package ru.mail.polis.dao;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.BuiltinComparator;
import org.rocksdb.Options;
import org.rocksdb.CompressionType;

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
        }
        catch (RocksDBException e) {
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
                final byte[] arrayValue =  ByteBufferUtils.fromByteToArray(value);
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
        }
        catch (RocksDBException  e) {
            throw new IOException("can't remove", e);
        }
    }

    private void dbClosing()
    {
        this.open.set(false);
    }

    @Override
    public void close() {
        if (!this.open.get()) {
            return;
        }
        dbClosing();
        db.close();
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
