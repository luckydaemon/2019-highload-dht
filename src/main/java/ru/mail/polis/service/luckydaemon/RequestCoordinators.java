package ru.mail.polis.service.luckydaemon;

import one.nio.http.Request;
import one.nio.http.HttpSession;
import one.nio.http.Response;

import com.google.common.base.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.DAOImpl;
import ru.mail.polis.dao.RecordTimestamp;

public class RequestCoordinators {
    private final DAOImpl dao;
    private final ClustersNodes clustersNodes;
    private final Map<String, HttpClient> clusterClients;
    private final Replicas defaultRF;
    private static final int NOT_FOUND = new Response(Response.NOT_FOUND).getStatus();
    private static final int INTERNAL_ERROR = new Response(Response.INTERNAL_ERROR).getStatus();

    private static final Log logger = LogFactory.getLog(RequestCoordinators.class);

    /**
     * Constructor.
     *
     * @param dao  dao
     * @param nodes  nodes in use
     * @param clusterClients map of client and nodes
     * @param defaultRF standard variant of rf
     */
    public RequestCoordinators(final DAO dao,
                               final ClustersNodes nodes,
                               final Map<String, HttpClient> clusterClients,
                               final Replicas defaultRF) {
        this.dao = (DAOImpl) dao;
        this.clustersNodes = nodes;
        this.clusterClients = clusterClients;
        this.defaultRF = defaultRF;
    }

    /**
     * Method for choosing next action based on request type.
     *@param proxied -determine if request sent by proxying or not
     * @param request - request itself
     * @param rf - rf
     * @param id - request id
     * @param session -session
     */
    public void coordinateRequest(final boolean proxied,
                                  final Request request,
                                  final Replicas rf,
                                  final String id,
                                  final HttpSession session) throws IOException {
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    session.sendResponse(get(proxied, rf, id));
                    break;
                case Request.METHOD_PUT:
                    session.sendResponse(upsert(proxied, request.getBody(), rf.getAck(), id));
                    break;
                case Request.METHOD_DELETE:
                    session.sendResponse(delete(proxied, rf.getAck(), id));
                    break;
                default:
                    session.sendError(Response.METHOD_NOT_ALLOWED, "wrong method /requestCoordinator");
                    break;
            }
        } catch (IOException e) {
            session.sendError(Response.GATEWAY_TIMEOUT, e.getMessage());
        }
    }

    private Response get(final boolean proxied, final Replicas rf, final String id) throws IOException {
        final String[] nodes;
        if (proxied) {
            nodes = new String[]{this.clustersNodes.getCurrentNodeId()};
        } else {
            nodes = this.clustersNodes.getReplics(rf.getFrom(), ByteBuffer.wrap(id.getBytes(Charsets.UTF_8)));
        }
        final List<CompletableFuture<RecordTimestamp>> futures = new ArrayList<>();
        final List<RecordTimestamp> responses = new ArrayList<>();
        for (final String node : nodes) {
            final CompletableFuture<RecordTimestamp> future;
            if (this.clustersNodes.isMe(node)) {
                final ByteBuffer byteBuffer = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
                future = CompletableFuture.supplyAsync(() -> daoGetWrapper(dao, byteBuffer));
            } else {
                future = clusterClients.get(node)
                        .sendAsync(HtttRequestBuilder.createGetHttpRequest(node, id),
                                   HttpResponse.BodyHandlers.ofByteArray())
                        .thenApply(response -> {
                            if (response.statusCode() == NOT_FOUND && response.body().length == 0) {
                                return RecordTimestamp.getEmptyRecord();
                            } else if (response.statusCode() != INTERNAL_ERROR) {
                                return RecordTimestamp.fromBytes(response.body());
                            }
                            return RecordTimestamp.getEmptyRecord();
                        });
            }
            futures.add(future);
        }

        final int countAcks = countAckForGetMthod(futures, responses);
        return processResponcessFromGetRequest(proxied, rf, nodes, countAcks, responses);
    }

    private Response delete(final boolean proxied, final int ack, final String id) {
        final ByteBuffer wrap = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        if (proxied) {
            try {
                dao.removeWithTimestamp(wrap);
                return new Response(Response.ACCEPTED, Response.EMPTY);
            } catch (IOException e) {
                return new Response(Response.INTERNAL_ERROR, e.toString().getBytes(Charsets.UTF_8));
            }
        }

        final List<CompletableFuture<String>> futures = new ArrayList<>();
        final String[] nodes = this.clustersNodes.getReplics(defaultRF.getFrom(), wrap);
        for (final String node : nodes) {
            final CompletableFuture<String> future;
            if (this.clustersNodes.isMe(node)) {
                future = CompletableFuture
                        .runAsync(() -> daoRemoveWrapper(dao, wrap))
                        .handle((avoid, throwable) ->
                                DaoMethodsHandlers.daoRemoveFutureHandler(throwable));
            } else {
                future = clusterClients.get(node)
                        .sendAsync(HtttRequestBuilder.createDeleteHttpRequest(node, id),
                                   HttpResponse.BodyHandlers.discarding())
                        .handle((response, throwable) ->
                                DaoMethodsHandlers.daoRemoveFutureHandlerWithResponse(response));
            }
            futures.add(future);

            final int countAcks = countAcks(futures,Response.ACCEPTED);
            if (countAcks >= ack) {
                return new Response(Response.ACCEPTED, Response.EMPTY);
            }
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    private Response upsert(final boolean proxied, final byte[] value, final int ack, final String id) {
        final ByteBuffer wrap = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        if (proxied) {
            try {
                dao.upsertWithTimestamp(wrap, ByteBuffer.wrap(value));
                return new Response(Response.CREATED, Response.EMPTY);
            } catch (IOException e) {
                return new Response(Response.INTERNAL_ERROR, e.toString().getBytes(Charsets.UTF_8));
            }
        }

        final List<CompletableFuture<String>> futures = new ArrayList<>();
        final String[] nodes = this.clustersNodes.getReplics(defaultRF.getFrom(), wrap);
        for (final String node : nodes) {
            final CompletableFuture<String> future;
            if (this.clustersNodes.isMe(node)) {
                future = CompletableFuture
                        .runAsync(() -> daoUpsertWrapper(dao, wrap, value))
                        .handle((aVoid, throwable) ->
                                DaoMethodsHandlers.daoUpsertFuturehandler(throwable));
            } else {
                future = clusterClients.get(node)
                        .sendAsync(HtttRequestBuilder.createPutHttpRequest(node, id, value),
                                HttpResponse.BodyHandlers.discarding())
                        .handle((response, throwable) ->
                                DaoMethodsHandlers.daoUpsertFuturehandlerWithResponse(response));
            }
            futures.add(future);
        }
        final int countAcks = countAcks(futures,Response.CREATED);
        if (countAcks >= ack) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            final String error = "not enough replicas";
            return new Response(Response.GATEWAY_TIMEOUT, error.getBytes(Charsets.UTF_8));
        }
    }

    private Response processResponcessFromGetRequest(final boolean proxied,
                                                     final Replicas rf,
                                                     final String[] nodes,
                                                     final int acks,
                                                     final List<RecordTimestamp> responses) {
        if (acks >= rf.getAck() || proxied) {
            final RecordTimestamp mergeResponse = RecordTimestamp.mergeRecords(responses);
            if (mergeResponse.isValue()) {
                return getResponse(proxied, nodes, mergeResponse);
            }
            if (mergeResponse.isDeleted()) {
                return new Response(Response.NOT_FOUND, mergeResponse.toBytes());
            }
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    @NotNull
    private Response getResponse(final boolean proxied,
                                        final String[] nodes,
                                        final  RecordTimestamp mergeResponse) {
        if (proxied) {
            return new Response(Response.OK, mergeResponse.toBytes());
        }
        if (nodes.length == 1) {
            return new Response(Response.OK, mergeResponse.getValueInByteFormat());
        }
        return new Response(Response.OK, mergeResponse.getValueInByteFormat());
    }

    private RecordTimestamp daoGetWrapper(final DAOImpl dao, final ByteBuffer byteBuffer) {
        try {
            return dao.getWithTimestamp(byteBuffer);
        } catch (IOException e) {
            logger.error(e);
        }
        return null;
    }

    private void daoUpsertWrapper(final DAOImpl dao, final ByteBuffer wrap, final byte[] value) {
        try {
            dao.upsertWithTimestamp(wrap, ByteBuffer.wrap(value));
        } catch (IOException e) {
            logger.error(e);
        }
    }

    private void daoRemoveWrapper(final DAOImpl dao, final ByteBuffer wrap) {
        try {
            dao.removeWithTimestamp(wrap);
        } catch (IOException e) {
            logger.error(e);
        }
    }

    private int countAcks(final List<CompletableFuture<String>> completableFutures,
                          final String response) {
        int acks = 0;
        for (final CompletableFuture<String> future : completableFutures) {
            try {
                if (future.get().equals(response)) {
                    acks++;
                }
            } catch (InterruptedException | ExecutionException e) {
                logger.error(e);
            }
        }
        return acks;
    }

    private int countAckForGetMthod(final List<CompletableFuture<RecordTimestamp>> completableFutures,
                            final List<RecordTimestamp> responses) {
        int acks = 0;
        for (final CompletableFuture<RecordTimestamp> future : completableFutures) {
            try {
                final RecordTimestamp timestamp = future.get();
                if (timestamp != null) {
                    responses.add(timestamp);
                    acks++;
                }
            } catch (InterruptedException | ExecutionException e) {
                logger.error(e);
            }
        }
        return acks;
    }
}
