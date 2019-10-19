package ru.mail.polis.service.luckydaemon;

import com.google.common.base.Charsets;

import one.nio.http.HttpSession;
import one.nio.http.HttpServer;
import one.nio.net.Socket;
import one.nio.http.Response;

import ru.mail.polis.Record;
import ru.mail.polis.dao.ByteBufferUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class StreamSession extends HttpSession {
    private static final byte[] CRLF = "\r\n".getBytes(Charsets.UTF_8);
    private static final byte[] DELIMITER = "\n".getBytes(Charsets.UTF_8);
    private static final byte[] EMPTY = "0\r\n\r\n".getBytes(Charsets.UTF_8);
    private Iterator<Record> iter;

    StreamSession(final HttpServer httpserver, final Socket socket){
        super(socket, httpserver);
    }

    /**prepare to stream
     *
     * @param iter data iterator
     * @throws IOException if something wrong
     */
    public void streamStart(final Iterator<Record> iter) throws IOException{
        this.iter = iter;
        if (handling == null) {
            throw new IOException("no handling");
        }
        final Response response = new Response(Response.OK);
        response.addHeader("Transfer-Encoding: chunked");
        writeResponse(response,false);
        nextPart();
    }

    private void nextPart() throws IOException {
        while (iter.hasNext() && queueHead == null) {
            final Record record = iter.next();
            final byte[] chunk = formAChunk(record);
            write(chunk, 0, chunk.length);
        }
        if (!iter.hasNext()) {
            closeStream();
        }
    }

    private void closeStream() throws IOException{
        write(EMPTY, 0, EMPTY.length);
        server.incRequestsProcessed();
        if (!keepAlive()) {
            scheduleClose();
        }
        if ((handling = pipeline.pollFirst()) != null) {
            if (handling == FIN) {
                scheduleClose();
            } else {
                server.handleRequest(handling, this);
            }
        }
        if (iter instanceof Closeable) {
            try {
                ((Closeable) iter).close();
            } catch (IOException exception) {
                throw new IOException("closing error", exception);
            }
        }
        iter = null;
    }

    private boolean keepAlive() {
        final var connection = handling.getHeader("Connection: ");
        return handling.isHttp11()
                ? !"close".equalsIgnoreCase(connection)
                : "Keep-Alive".equalsIgnoreCase(connection);
    }

    private byte[] formAChunk(final Record record){
        final byte[] key = ByteBufferUtils.fromByteToArray(record.getKey());
        final byte[] value = ByteBufferUtils.fromByteToArray(record.getValue());
        final int payloadLength = key.length + value.length + DELIMITER.length;
        final String payloadHex = Integer.toHexString(payloadLength);
        final int chunksLength = payloadLength + payloadHex.length() + CRLF.length * 2;
        final byte[] chunk = new byte[chunksLength];
        final ByteBuffer buffer = ByteBuffer.wrap(chunk);
        buffer.put(payloadHex.getBytes(Charsets.UTF_8));
        buffer.put(CRLF);
        buffer.put(key);
        buffer.put(DELIMITER);
        buffer.put(value);
        buffer.put(CRLF);
        return chunk;
    }

    @Override
    protected void processWrite() throws Exception {
        super.processWrite();
        nextPart();
    }
}
