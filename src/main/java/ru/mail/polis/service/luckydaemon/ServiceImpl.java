package ru.mail.polis.service.luckydaemon;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import one.nio.http.*;
import one.nio.net.Socket;
import one.nio.net.ConnectionString;
import one.nio.server.AcceptorConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
    private final int clusterSize;
    private final Replicas defaultRF;
    private static final String PROXY_HEADER = "X-OK-Proxy: True";


    /**
     * Constructor.
     *
     * @param config server config
     * @param dao  dao
     * @param nodes  nodes in use
     * @param clusterClients map of client and nodes
     */
    public ServiceImpl(@NotNull final HttpServerConfig config,
                       @NotNull final DAO dao,
                       @NotNull final ClustersNodes nodes,
                       @NotNull final Map<String, HttpClient> clusterClients) throws IOException {
        super(config);
        this.dao = dao;
        this.exec = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryBuilder().setNameFormat("exec-%d").build());
        this.nodes = nodes;
        this.clusterClients = clusterClients;
        this.clusterSize = nodes.getNodes().size();
        this.defaultRF = new Replicas(nodes.getNodes().size() / 2 + 1, nodes.getNodes().size());
    }

    /**
     * Method to set parameters and create an object.
     *
     * @param port -port
     * @param dao  dao
     * @param nodes  nodes
     */
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
        return Response.ok("OK");
    }

    /**
     * Access to entity.
     *
     * @param id   id
     * @param request  request
     */
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
        boolean isProxy = false;
        if (request.getHeader(PROXY_HEADER) != null) {
            isProxy = true;
        }
        final Replicas rf = Replicas.calculateRF(replicas, clusterSize, session, defaultRF);
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        if (isProxy || nodes.getNodes().size() > 1) {
            final RequestCoordinators Coordinator = new RequestCoordinators(dao, nodes, clusterClients, isProxy);
            final String[] replicaClusters;
            if (isProxy) {
                replicaClusters = new String[]{nodes.getCurrentNodeId()};
            } else {
                replicaClusters = nodes.getReplics(rf.getFrom(), key);
            }
            Coordinator.coordinateRequest(replicaClusters, request, rf.getAck(), session);
        } else {
            executeAsyncRequest(request, key, session);
        }
    }
    
    private void executeAsyncRequest(final Request request, final ByteBuffer key,
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

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
                session.sendError(Response.BAD_REQUEST, "wrong path");
    }

    private void responseSend(@NotNull final Resp response, @NotNull final HttpSession session){
        exec.execute(() -> {
            try {
                session.sendResponse(response.response());
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, null);
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
    public HttpSession createSession(final Socket socket){
        return new StreamSession(this, socket);
    }

    @FunctionalInterface
    interface Resp {
        Response response() throws IOException;
    }

}
