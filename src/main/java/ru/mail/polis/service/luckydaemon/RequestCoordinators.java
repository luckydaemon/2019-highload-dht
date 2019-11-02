package ru.mail.polis.service.luckydaemon;

import one.nio.http.HttpClient;
import one.nio.http.Request;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.http.HttpException;
import one.nio.pool.PoolException;

import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.google.common.base.Charsets;
import org.jetbrains.annotations.NotNull;

import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.DAOImpl;
import ru.mail.polis.dao.RecordTimestamp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.NoSuchElementException;

public class RequestCoordinators {
    @NotNull
    private final DAOImpl dao;
    private final ClustersNodes nodes;
    private final Map<String, HttpClient> clusterClients;
    private final boolean proxied;

    private static final Logger logger = Logger.getLogger(RequestCoordinators.class.getName());
    private static final String PROXY_HEADER = "X-OK-Proxy: True";
    private static final String ENTITY_HEADER = "/v0/entity?id=";

    /**
     * Get id where replicas will be.
     *
     * @param dao - dao
     * @param nodes nodes
     * @param clusterClients current clients of cluster
     * @param proxied -determine if request sent by proxying or not
     */
    public RequestCoordinators(@NotNull final DAO dao,
                               final ClustersNodes nodes,
                               final Map<String, HttpClient> clusterClients,
                               final boolean proxied) {
        this.dao = (DAOImpl) dao;
        this.nodes = nodes;
        this.clusterClients = clusterClients;
        this.proxied = proxied;
    }

    /**
     * controll put request.
     *
     * @param replicaNodes - nodes where created perlicas will be placed
     * @param request request
     * @param acks amount of acks
     * @param proxied -determine if request sent by proxying or not
     * @return Response
     */
    public Response putRequestCoordinate(final String[] replicaNodes, final Request request,
                                  final int acks, final boolean proxied) throws IOException {
        final String id = request.getParameter("id=");
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        int asks = 0;
        for (final String node : replicaNodes) {
            try {
                if (node.equals(nodes.getCurrentNodeId())) {
                    dao.upsertWithTimestamp(key, ByteBuffer.wrap(request.getBody()));
                    asks++;
                } else {
                    request.addHeader(PROXY_HEADER);
                    final Response resp = clusterClients.get(node)
                            .put(ENTITY_HEADER + id, request.getBody(), PROXY_HEADER);
                    if (resp.getStatus() == 201) {
                        asks++;
                    }
                }
            } catch (IOException | HttpException | PoolException | InterruptedException e) {
                logger.log(Level.SEVERE, "error in putting", e);
            }
        }
        if (asks >= acks || proxied) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    /**
     * controll get request.
     *
     * @param replicaNodes - nodes where created perlicas will be placed
     * @param request request
     * @param acks amount of acks
     * @param proxied -determine if request sent by proxying or not
     * @return Response
     */
    public Response getRequestCoordinate(final String[] replicaNodes, final Request request,
                                  final int acks, final boolean proxied) throws IOException {
        final String id = request.getParameter("id=");
        final var key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        int asks = 0;
        final List<RecordTimestamp> responses = new ArrayList<>();
        for (final String node : replicaNodes) {
            try {
                Response respGet;
                if (node.equals(nodes.getCurrentNodeId())) {
                    respGet = getMethodWrapper(key);

                } else {
                    request.addHeader(PROXY_HEADER);
                    respGet = clusterClients.get(node)
                            .get(ENTITY_HEADER + id, PROXY_HEADER);
                }
                if (respGet.getStatus() == 404 && respGet.getBody().length == 0) {
                    responses.add(RecordTimestamp.getEmptyRecord());
                } else if (respGet.getStatus() == 500) {
                    continue;
                } else {
                    responses.add(RecordTimestamp.fromBytes(respGet.getBody()));
                }
                asks++;
            } catch (HttpException | PoolException | InterruptedException e) {
                logger.log(Level.SEVERE, "error in getting", e);
            }
        }
        if (asks >= acks || proxied) {
            return responsesProcessing(replicaNodes, responses);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response responsesProcessing(final String[] replicaNodes,
                                      final List<RecordTimestamp> responses) throws IOException {
        final RecordTimestamp mergedResponce = RecordTimestamp.mergeRecords(responses);
        if(mergedResponce.isValue()) {
            if(!proxied && replicaNodes.length == 1) {
                return new Response(Response.OK, mergedResponce.getValueInByteFormat());
            } else if (proxied && replicaNodes.length == 1) {
                return new Response(Response.OK, mergedResponce.toBytes());
            } else {
                return new Response(Response.OK, mergedResponce.getValueInByteFormat());
            }
        } else if (mergedResponce.isDeleted()) {
            return new Response(Response.NOT_FOUND, mergedResponce.toBytes());
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    /**
     * controll delete request.
     *
     * @param replicaNodes - nodes where created perlicas will be placed
     * @param request request
     * @param acks amount of acks
     * @param proxied -determine if request sent by proxying or not
     * @return Response
     */
    public Response deleteRequestCoordinate(final String[] replicaNodes, final Request request,
                                     final int acks, final boolean proxied) throws IOException {
        final String id = request.getParameter("id=");
        final var key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        int asks = 0;
        for (final String node : replicaNodes) {
            try {
                if (node.equals(nodes.getCurrentNodeId())) {
                    dao.removeWithTimestamp(key);
                    asks++;
                } else {

                    request.addHeader(PROXY_HEADER);
                    final Response resp = clusterClients.get(node)
                            .delete(ENTITY_HEADER + id, PROXY_HEADER);
                    if (resp.getStatus() == 202) {
                        asks++;
                    }
                }
            } catch (IOException | HttpException | InterruptedException | PoolException e) {
                logger.log(Level.SEVERE, "error in deleting ", e);
            }
        }
        if (asks >= acks || proxied) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    /**
     * determine action according to request.
     *
     * @param replicaClusters - nodes where created perlicas will be placed
     * @param request request
     * @param acks amount of acks
     * @param session - current session
     */
    public void coordinateRequest(final String[] replicaClusters, final Request request,
                                  final int acks, final HttpSession session) throws IOException {
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    session.sendResponse(getRequestCoordinate(replicaClusters, request, acks, proxied));
                    return;
                case Request.METHOD_PUT:
                    session.sendResponse(putRequestCoordinate(replicaClusters, request, acks, proxied));
                    return;
                case Request.METHOD_DELETE:
                    session.sendResponse(deleteRequestCoordinate(replicaClusters, request, acks, proxied));
                    return;
                default:
                    session.sendError(Response.METHOD_NOT_ALLOWED, "not supported method");
                    return;
            }
        } catch (IOException e) {
            session.sendError(Response.GATEWAY_TIMEOUT, e.getMessage());
        }
    }

    @NotNull
    private Response getMethodWrapper(final ByteBuffer key) throws IOException {
        try {
            final byte[] res = copyFromByteBuffer(key);
            return new Response(Response.OK, res);
        } catch (NoSuchElementException exp) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private byte[] copyFromByteBuffer(@NotNull final ByteBuffer key) throws IOException {
        final RecordTimestamp record = dao.getWithTimestamp(key);
        if(record.isMissing()){
            throw new NoSuchElementException("no element");
        }
        return record.toBytes();
    }
}
