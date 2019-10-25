package ru.mail.polis.service.luckydaemon;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.pool.PoolException;
import one.nio.net.ConnectionString;
import one.nio.server.AcceptorConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class ServiceImpl extends HttpServer implements Service {
    private static final Log logger = LogFactory.getLog(ServiceImpl.class);
    private final DAO dao;
    private final Executor exec;
    private final ClustersNodes nodes;
    private final Map<String, HttpClient> clusterClients;

    public ServiceImpl(final HttpServerConfig config,
                       @NotNull final DAO dao,
                       @NotNull final ClustersNodes nodes,
                       @NotNull final Map<String, HttpClient> clusterClients) throws IOException {
        super(config);
        this.dao = dao;
        this.exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryBuilder().setNameFormat("exec").build());
        this.nodes = nodes;
        this.clusterClients = clusterClients;
    }

    public static Service create(final int port, @NotNull final DAO dao,
                                 @NotNull final ClustersNodes nodes) throws IOException {
        final var acceptor = new AcceptorConfig();
        final var config = new HttpServerConfig();
        acceptor.port = port;
        config.acceptors = new AcceptorConfig[]{acceptor};
        config.maxWorkers = Runtime.getRuntime().availableProcessors();
        config.queueTime = 10;
        final Map<String, HttpClient> clusterClients = new HashMap<>();
        for (final String st : nodes.getNodes()) {
            if (!nodes.getCurrentNodeId().equals(st) && !clusterClients.containsKey(st)) {
                clusterClients.put(st, new HttpClient(new ConnectionString(st + "?timeout=100")));
            }
        }
        return new ServiceImpl(config, dao, nodes, clusterClients);
    }


    @Path("/v0/status")
    public Response status() {
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
            session.sendError(Response.BAD_REQUEST, "no id");
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        final String keyCluster= nodes.keyCheck(key);
            if (nodes.getCurrentNodeId().equals(keyCluster)) {
                try {
                    switch (request.getMethod()) {
                        case Request.METHOD_GET:
                            responseSend(() -> get(key), session);
                            break;

                        case Request.METHOD_PUT:
                            responseSend(() -> put(key, request), session);
                            break;

                        case Request.METHOD_DELETE:
                            responseSend(() -> delete(key), session);
                            break;

                        default:
                            session.sendError(Response.METHOD_NOT_ALLOWED, "wrong method");
                            break;
                    }
                } catch (IOException exception) {
                    session.sendError(Response.INTERNAL_ERROR, " ");
                }
            } else {
                responseSend(()->sendForward(nodes.keyCheck(key), request), session);
            }
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

    private void responseSend(@NotNull final Resp response, @NotNull final HttpSession session){
        exec.execute(() -> {
            try {
                session.sendResponse(response.response());
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, null);
                } catch (IOException ex) {
                    logger.error(e);
                }
            }
        });
    }

    /** access to entities.
     *@param  start first key
     *@param end  last key
     *@param session session
     */
    @Path("/v0/entities")
    public void entities(@Param("start") final String start,
                         @Param("end") final String end,
                         @NotNull final HttpSession session) throws IOException {
        if (start == null || start.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "No start");
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
                try {
                    session.sendError(Response.BAD_REQUEST, exception.getMessage());
                } catch (IOException e) {
                    logger.error("Something wrong", e);
                }

            }
        });
    }

    @Override
    public HttpSession createSession(final Socket socket){
        return new StreamSession(this,  socket);
    }

    private Response sendForward(@NotNull final String cluster, final Request request) throws IOException {
        try {
            return clusterClients.get(cluster).invoke(request);
        } catch (InterruptedException | PoolException | HttpException e) {
            logger.error("error while forwarding", e);
            return new Response(Response.INTERNAL_ERROR, "internal error".getBytes(Charsets.UTF_8));
        }
    }

    @FunctionalInterface
    interface Resp {
        Response response() throws IOException;
    }
}
