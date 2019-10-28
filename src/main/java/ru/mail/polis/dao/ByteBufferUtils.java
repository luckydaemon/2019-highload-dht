package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;

public final class ByteBufferUtils {

    private ByteBufferUtils(){}

    /**
     * reverse shift bytes.
     *
     * @param key   value to shift
     * @return  shifted value
     */
    public static ByteBuffer revShift(@NotNull final byte[] key) {
        final byte[] copy = Arrays.copyOf(key, key.length);
        for (int i = 0; i < copy.length; i++) {
            copy[i] += Byte.MIN_VALUE;
        }
        return ByteBuffer.wrap(copy);
    }

    /**
     * transform buffer to array.
     *
     * @param buffer   value to transform
     * @return  array
     */
    public static byte[] fromByteToArray(@NotNull final ByteBuffer buffer) {
        final ByteBuffer copy = buffer.duplicate();
        final byte[] array = new byte[copy.remaining()];
        copy.get(array);
        return array;
    }

    /**
     * shift bytes.
     *
     * @param key   value to shift
     * @return  shifted value
     */
    public static byte[] shift(@NotNull final ByteBuffer key) {
        final byte[] arrayKey = fromByteToArray(key);
        for (int i = 0; i < arrayKey.length; i++) {
            arrayKey[i] -= Byte.MIN_VALUE;
        }
        return arrayKey;
    }
}
