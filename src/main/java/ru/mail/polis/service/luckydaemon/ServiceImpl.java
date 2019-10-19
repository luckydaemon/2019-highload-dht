package ru.mail.polis.service.luckydaemon;

import com.google.common.base.Charsets;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.server.RejectedSessionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;

import one.nio.server.AcceptorConfig;

import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public class ServiceImpl extends HttpServer implements Service {
    private static final Log logger = LogFactory.getLog(ServiceImpl.class);
    private final DAO dao;

    public ServiceImpl(final int port, @NotNull final DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
    }

    private static HttpServerConfig getConfig(final int port) {
        if (port <= 1024 || port >= 66535) {
            throw new IllegalArgumentException("Invalid port");
        }
        final AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        final HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ acceptor };
        return config;
    }

    @Path("/v0/status")
    public Response status()
    {
        return new Response(Response.OK, Response.EMPTY);
    }

     /** access to entity.
      *@param id   id
      *@param request  request
    */
    @Path("/v0/entity")
    public void entity(@Param("id") final String id,
                       @NotNull final Request request,
                       final HttpSession session) throws IOException {
        if (id == null || id.isEmpty()) {
            responseSend(new Response(Response.BAD_REQUEST, "require id".getBytes(Charsets.UTF_8)), session);
        }
        asyncExecute(() -> {
            final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    responseSend(get(key),session);
                    break;

                case Request.METHOD_PUT:
                    responseSend(put(key, request), session);
                    break;

                case Request.METHOD_DELETE:
                    responseSend(delete(key), session);
                    break;

                default:
                    responseSend(new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY), session);
                    break;
            }
        }
        catch (IOException exception) {
                responseSend(new Response(Response.INTERNAL_ERROR, Response.EMPTY), session);
        }
        });
    }

    private Response get(final ByteBuffer key) throws IOException {
        try {
            final ByteBuffer value = dao.get(key);
            final byte[] body = new byte[value.remaining()];
            value.get(body);
            return new Response(Response.OK, body);
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, "Key not found".getBytes(Charsets.UTF_8));
        }
    }

    private Response put(final ByteBuffer key,final Request request) throws IOException {
        dao.upsert(key, ByteBuffer.wrap(request.getBody()));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response delete(final ByteBuffer key) throws IOException {
        dao.remove(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }



    private void responseSend(@NotNull final Response response, @NotNull final HttpSession session){
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            logger.error(e);
        }
    }
    /** access to entities.
     *@param  start first key
     *@param end  last key
     *@param session session
     */
    @Path("/v0/entities")
    public void entities(@Param("start") final String start, @Param("end") final String end, @NotNull final HttpSession session){
        if (start == null || start.isEmpty()) {
            responseSend(new Response(Response.BAD_REQUEST, "need start".getBytes(Charsets.UTF_8)), session);
            return;
        }
        final var firstBytes = ByteBuffer.wrap(start.getBytes(Charsets.UTF_8));
        ByteBuffer lastBytes = null;
        if (end != null && !end.isEmpty()) {
            lastBytes = ByteBuffer.wrap(end.getBytes(Charsets.UTF_8));
        }
        final var latestBytes = lastBytes;
        final var streamSession = (StreamSession) session;
        asyncExecute(() -> {
            try {
                final var rangeIter = dao.range(firstBytes,latestBytes);
                streamSession.streamStart(rangeIter);
            } catch (IOException exception) {
                    responseSend(new Response(Response.INTERNAL_ERROR, "error".getBytes(Charsets.UTF_8)), session);
            }
        });
    }

    @Override
    public HttpSession createSession(final Socket socket) throws RejectedSessionException {
        return new StreamSession(this,  socket);
    }
}
