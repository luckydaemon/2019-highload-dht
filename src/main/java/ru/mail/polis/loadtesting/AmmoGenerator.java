package ru.mail.polis.loadtesting;

import com.google.common.base.Charsets;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.io.FileOutputStream;
import java.util.Random;

public final class AmmoGenerator {
    private static final int LENGTH = 256;
    private static final String CRLF = "\r\n";
    private static final String URL = "/v0/entity?id=";
    private static Random random = new Random();

    private AmmoGenerator() {
    }

    private static void createPutRequest(final long keyLong, final OutputStream outputStream) throws IOException {
        final String key = String.valueOf(keyLong);
        final byte[] value = new byte[LENGTH];
        random.nextBytes(value);
        final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (Writer requestWriter = new OutputStreamWriter(byteStream, Charsets.UTF_8)) {
            requestWriter.write("PUT " + URL + key + " HTTP/1.1" + CRLF);
            requestWriter.write("Content-Length: " + value.length + CRLF);
            requestWriter.write(CRLF);
        }
            byteStream.write(value);
            outputStream.write(Integer.toString(byteStream.size()).getBytes(Charsets.UTF_8));
            outputStream.write(" PUT\n".getBytes(Charsets.UTF_8));
            byteStream.writeTo(outputStream);
            outputStream.write(CRLF.getBytes(Charsets.UTF_8));
    }

    private static void createGetRequest(final long keyLong, final OutputStream outputStream) throws IOException {
        final String key = String.valueOf(keyLong);
        final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (Writer requestWriter = new OutputStreamWriter(byteStream, Charsets.UTF_8)) {
            requestWriter.write("GET " + URL + key + " HTTP/1.1" + CRLF);
            requestWriter.write(CRLF);
        }
        outputStream.write(Integer.toString(byteStream.size()).getBytes(Charsets.UTF_8));
        outputStream.write(" get\n".getBytes(Charsets.UTF_8));
        byteStream.writeTo(outputStream);
        outputStream.write(CRLF.getBytes(Charsets.UTF_8));
    }

    private static void putUnique(final long amount) throws IOException {
        final OutputStream outputStream = new FileOutputStream("put");
            for (long i = 1; i < amount; i++) {
                createPutRequest(i, outputStream);
            }
        outputStream.close();
    }

    /**
     * Create put requests with 10% overwrite.
     *
     * @param amount - needed amount of request
     */
   public static void put10Overwrite(final long amount) throws IOException {
       int nextKey = 0;
       final OutputStream outputStream = new FileOutputStream("put_10");
            for (long i = 0; i < amount; i++) {
                final boolean isOverwrite = random.nextInt(10) == 0;
                if (isOverwrite) {
                    final long key = random.nextInt(nextKey);
                    createPutRequest(key, outputStream);
                } else {
                    createPutRequest(nextKey, outputStream);
                    nextKey++;
                }
            }
       outputStream.close();
    }

    /**
     * Create get request.
     *
     * @param amount - needed amount of request
     */
    public static void get(final int amount) throws IOException {
        final OutputStream outputStream = new FileOutputStream("get");
            for (int i = 0; i < amount; i++) {
                final int key = random.nextInt(amount);
                createGetRequest(key, outputStream);
            }
        outputStream.close();
    }

    /**
     * Create get request with offset.
     *
     * @param amount - needed amount of request
     */
    public static void getNew(final long amount) throws IOException {
        long key = 0;
        final OutputStream outputStream = new FileOutputStream("getNew");
            for (long i = 0; i < amount; i++) {
                boolean isDone = false;
                while (!isDone) {
                    final double gauss = random.nextGaussian();
                    final double div = gauss / 3;
                    if (div < -1 || div > 0) continue;
                    final double max = div * (amount + 1);
                    final long rounded = Math.round(max);
                    final long abs = Math.abs(rounded);
                    key = -abs + amount + 1;
                    if (key > amount) continue;
                    isDone = true;
                }
                createGetRequest(key, outputStream);
            }
        outputStream.close();
    }

    /**
     * Create mixed requests.
     *
     * @param amount - needed amount of request
     */
    public static void mixed(final long amount) throws IOException {
        int getKey = 1;
        final OutputStream outputStream = new FileOutputStream("mix");
            for (int i = 0; i < amount; i++) {
                final boolean choice = random.nextBoolean();
                if (choice) {
                    createGetRequest(getKey, outputStream);
                    getKey++;
                } else {
                    final int key = random.nextInt(getKey);
                    createPutRequest(key, outputStream);
                }
            }
        outputStream.close();
    }

    /**
     *Create all ammo.
     *
     */
    public static void main(final String[] args) throws IOException {
        putUnique(50000);
        mixed(50000);
        get(50000);
        getNew(50000);
        put10Overwrite(50000);
    }
}
