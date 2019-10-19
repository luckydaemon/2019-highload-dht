package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import org.rocksdb.RocksIterator;

import ru.mail.polis.Record;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class RocksDBIterator implements Iterator<Record>, Closeable {
    private final RocksIterator iterator;

    public RocksDBIterator(@NotNull final RocksIterator iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext()
    {
        return iterator.isValid();
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            throw new IllegalStateException("RocksDBIterator error");
        }
        final var keyByteArray = iterator.key();
        final ByteBuffer packedKey = ByteBufferUtils.revShift(keyByteArray);
        final var valueByteArray = iterator.value();
        final var value = ByteBuffer.wrap(valueByteArray);
        final var record = Record.of(packedKey, value);
        iterator.next();
        return record;
    }

    @Override
    public void close() {
        iterator.close();
    }
}
