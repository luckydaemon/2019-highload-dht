package ru.mail.polis.service.luckydaemon;

import com.google.common.base.Charsets;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.http.Request;
import one.nio.net.Socket;
import one.nio.http.Path;
import one.nio.http.Param;

import one.nio.server.AcceptorConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;

import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.DAOImpl;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

import java.net.http.HttpClient;
import java.util.concurrent.Executors;

public class ServiceImpl extends HttpServer implements Service {
    private final DAOImpl dao;
    private final Executor exec;
    private final int size;
    private final Replicas defaultRF;
    private final RequestCoordinators coord;
    private static final String PROXY_HEADER = "PROXY_HEADER";

    private static final Log logger = LogFactory.getLog(ServiceImpl.class);

    /**
     * Constructor.
     *
     * @param config server config
     * @param dao  dao
     * @param nodes  nodes in use
     * @param ClusterClients map of client and nodes
     */
    public ServiceImpl(
            @NotNull final HttpServerConfig config,
            final DAO dao,
            final ClustersNodes nodes,
            final Map<String, HttpClient> ClusterClients
    ) throws IOException {
        super(config);
        this.dao = (DAOImpl) dao;
        this.exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryBuilder().setNameFormat("exec-%d").build());
        this.size = nodes.getNodes().size();
        this.defaultRF = new Replicas(nodes.getNodes().size() / 2 + 1, size);
        this.coord = new RequestCoordinators(dao, nodes, ClusterClients,defaultRF);
    }
    /**
     * Method to set parameters and create an object.
     *
     * @param port -port
     * @param dao  dao
     * @param nodes  nodes
     */
    public static Service create(final int port,
                                 @NotNull final DAO dao,
                                 @NotNull final ClustersNodes nodes) throws IOException {
        final var acceptor = new AcceptorConfig();
        final var config = new HttpServerConfig();
        acceptor.port = port;
        config.acceptors = new AcceptorConfig[]{acceptor};
        config.maxWorkers = Runtime.getRuntime().availableProcessors();
        config.queueTime = 10;
        final Map<String, HttpClient> ClusterClients = new HashMap<>();
        for (final String node : nodes.getNodes()) {
            if (nodes.isMe(node)) {
                continue;
            }
            assert !ClusterClients.containsKey(node);
            ClusterClients.put(node, HttpClient.newBuilder().build());
        }
        return new ServiceImpl( config,  dao, nodes, ClusterClients);
    }

    @Path("/v0/status")
    public Response status() {
        return Response.ok("OK");
    }

    @Override
    public HttpSession createSession(final Socket socket) {
        return new StreamSession( this, socket);
    }

    @Path("/v0/entity")
    public void entity( @Param("id") final String id,
                        @Param("replicas") final String replicas,
                        @NotNull final Request request,
                        final HttpSession session) throws IOException {
        if (request.getURI().equals("/v0/entity")) {
            session.sendError(Response.BAD_REQUEST, "no parameters");
            return;
        }
        if (id == null || id.isEmpty()) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        boolean proxied = true;
        if (request.getHeader(PROXY_HEADER) == null) {
            proxied = false;
        }
        final Replicas rf;
        if (replicas == null) {
            rf =defaultRF;
        } else {
            rf = Replicas.of(replicas);
        }
        if (rf.getAck() < 1 || rf.getFrom() < rf.getAck() || rf.getFrom() > size) {
            session.sendError(Response.BAD_REQUEST, "invalid value of rf /serviceimpl");
        }

        if (proxied || size > 1) {
                coord.coordinateRequest(proxied, request, rf, id, session);
        } else {
            final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
            executeAsyncRequest(request, key, session);
            }
    }

    private void executeAsyncRequest(final Request request,
                                     final ByteBuffer key,
                                     final HttpSession session) throws IOException {
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                responseSend(() -> get(key), session);
                return;
            case Request.METHOD_PUT:
                responseSend(() -> put(key, request), session);
                return;
            case Request.METHOD_DELETE:
                responseSend(() -> delete(key), session);
                return;
            default:
                session.sendError(Response.METHOD_NOT_ALLOWED, "Wrong method");
                return;
        }
    }

    private Response get(final ByteBuffer key) throws IOException {
        try {
            final byte[] res = copyFromByteBuffer(key);
            return new Response(Response.OK, res);
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private byte[] copyFromByteBuffer(@NotNull final ByteBuffer key) throws IOException {
        final ByteBuffer dct = dao.get(key).duplicate();
        final byte[] res = new byte[dct.remaining()];
        dct.get(res);
        return res;
    }

    private Response put(final ByteBuffer key,final Request request) throws IOException {
        dao.upsert(key, ByteBuffer.wrap(request.getBody()));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response delete(final ByteBuffer key) throws IOException {
        dao.remove(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private void responseSend(@NotNull final Resp response, @NotNull final HttpSession session){
        exec.execute(() -> {
            try {
                session.sendResponse(response.response());
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, "error while sending async response");
                } catch (IOException ex) {
                    logger.error(ex);
                }
            }
        });
    }

    /**
     * Access to entities.
     *
     * @param  start first key
     * @param end  last key
     * @param session session
     */
    @Path("/v0/entities")
    public void entities(@Param("start") final String start,
                         @Param("end") final String end,
                         @NotNull final HttpSession session) throws IOException {
        if (start == null || start.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "no start");
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
                    logger.error("something wrong in entities /serviceimpl", e);
                }

            }
        });
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        session.sendError(Response.BAD_REQUEST, "wrong path");
    }

    @FunctionalInterface
    interface Resp {
        Response response() throws IOException;
    }
}
