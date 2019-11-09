package ru.mail.polis.service.luckydaemon;

import one.nio.http.HttpClient;
import one.nio.http.Request;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.http.HttpException;
import one.nio.pool.PoolException;

import com.google.common.base.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetbrains.annotations.NotNull;

import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.DAOImpl;
import ru.mail.polis.dao.RecordTimestamp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.NoSuchElementException;


public class RequestCoordinators {
    @NotNull
    private final DAOImpl dao;
    private final ClustersNodes nodes;
    private final Map<String, HttpClient> clusterClients;
    private final boolean proxied;

    private static final Log logger = LogFactory.getLog(ServiceImpl.class);
    private static final String PROXY_HEADER = "X-OK-Proxy: True";
    private static final String URL = "/v0/entity?id=";

    /**
     * Get id where replicas will be.
     *
     * @param dao - dao
     * @param nodes nodes
     * @param clusterClients current clients of cluster
     * @param isProxy -determine if request sent by proxying or not
     */
    public RequestCoordinators(@NotNull final DAO dao,
                               final ClustersNodes nodes,
                               final Map<String, HttpClient> clusterClients,
                               final boolean isProxy) {
        this.dao = (DAOImpl) dao;
        this.nodes = nodes;
        this.clusterClients = clusterClients;
        this.proxied = isProxy;
    }

    /**
     * Control put request.
     *
     * @param replicaNodes - nodes where created perlicas will be placed
     * @param request request
     * @param acks amount of acks
     * @param isProxy -determine if request sent by proxying or not
     * @return Response
     */
    public Response putRequestCoordinate(final String[] replicaNodes,
                                         final Request request,
                                         final int acks,
                                         final boolean isProxy) throws IOException {
        final String id = request.getParameter("id=");
        final var key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        if (isProxy) {
            dao.upsertWithTimestamp(key, ByteBuffer.wrap(request.getBody()));
            return new Response(Response.CREATED, Response.EMPTY);
        }
        int acksCounter = 0;
        for (final String node : replicaNodes) {
            try {
                if (node.equals(nodes.getCurrentNodeId())) {
                    dao.upsertWithTimestamp(key, ByteBuffer.wrap(request.getBody()));
                    acksCounter++;
                } else {
                    final Response response = clusterClients.get(node)
                            .put(URL + id, request.getBody(), PROXY_HEADER);
                    if (response.getStatus() == 201) {
                        acksCounter++;
                    }
                }
            } catch (HttpException | PoolException | InterruptedException e) {
                logger.error("error in putting", e);
            }
        }
        if (acksCounter >= acks) {
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    /**
     * Control get request.
     *
     * @param replicaNodes - nodes where created perlicas will be placed
     * @param id from request
     * @param acks amount of acks
     * @param isProxy -determine if request sent by proxying or not
     * @return Response
     */
    public Response getRequestCoordinate(final String[] replicaNodes,
                                         final String id,
                                         final int acks,
                                         final boolean isProxy) throws IOException {
        final var key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        final List<RecordTimestamp> responses = new ArrayList<>(replicaNodes.length);
        if (isProxy) {
           try {
               return processIfProxy(replicaNodes, id, responses, key);
           } catch (IOException | HttpException | PoolException | InterruptedException e) {
               logger.error("error in putting", e);

           }
        }
        int acksCounter = 0;
        for (final String node : replicaNodes) {
            try {
                responses.addAll(processNode(node, id, key));
                acksCounter++;
            } catch (HttpException | PoolException | InterruptedException e) {
                logger.error("error in getting", e);
            }
        }
        if (acksCounter >= acks) {
            return responsesProcessing(responses);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response responsesProcessing(final List<RecordTimestamp> responses) {
        final RecordTimestamp mergedResponse = RecordTimestamp.mergeRecords(responses);
        if(mergedResponse.isValue()) {
                return new Response(Response.OK, mergedResponse.getValueInByteFormat());
        } else {
            return mergedResponceIsDeletedChecker(mergedResponse);
        }
    }

    private Response mergedResponceIsDeletedChecker (final RecordTimestamp response) {
        if (response.isDeleted()) {
            return new Response(Response.NOT_FOUND, response.toBytes());
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }
    
    /**
     * Control delete request.
     *
     * @param replicaNodes - nodes where created perlicas will be placed
     * @param id from request
     * @param acks amount of acks
     * @param isProxy -determine if request sent by proxying or not
     * @return Response
     */
    public Response deleteRequestCoordinate(final String[] replicaNodes,
                                            final String id,
                                            final int acks,
                                            final boolean isProxy) throws IOException {
        final var key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        if (isProxy) {
            dao.removeWithTimestamp(ByteBuffer.wrap(id.getBytes(Charsets.UTF_8)));
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }
        int acksCounter = 0;
        for (final String node : replicaNodes) {
            try {
                if (node.equals(nodes.getCurrentNodeId())) {
                    dao.removeWithTimestamp(key);
                    acksCounter++;
                } else {
                    final Response response = clusterClients.get(node)
                            .delete(URL + id, PROXY_HEADER);
                    if (response.getStatus() == 202) {
                        acksCounter++;
                    }
                }
            } catch ( HttpException | InterruptedException | PoolException e) {
                logger.error("error in deleting", e);
            }
        }
        if (acksCounter >= acks) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    /**
     * Determine action according to request.
     *
     * @param replicaClusters - nodes where created perlicas will be placed
     * @param request request
     * @param acks amount of acks
     * @param session - current session
     */
    public void coordinateRequest(final String[] replicaClusters,
                                  final Request request,
                                  final int acks,
                                  final HttpSession session) throws IOException {
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET: {
                    final String id = request.getParameter("id=");
                    session.sendResponse(getRequestCoordinate(replicaClusters, id, acks, proxied));
                    return;
                    }
                case Request.METHOD_PUT: {
                    session.sendResponse(putRequestCoordinate(replicaClusters, request, acks, proxied));
                    return;
                    }
                case Request.METHOD_DELETE:{
                    final String id = request.getParameter("id=");
                    session.sendResponse(deleteRequestCoordinate(replicaClusters, id, acks, proxied));
                    return;
                    }
                default:{
                    session.sendError(Response.METHOD_NOT_ALLOWED, "not supported method");
                    return;
                    }
            }
        } catch (IOException e) {
            session.sendError(Response.GATEWAY_TIMEOUT, e.getMessage());
        }
    }

    @NotNull
    private Response getMethodWrapper(final ByteBuffer key) throws IOException {
        try {
            final RecordTimestamp record = dao.getWithTimestamp(key);
            if(record.isMissing()){
                throw new NoSuchElementException("no element");
            }
            final byte[] response = record.toBytes();
            return new Response(Response.OK, response);
        } catch (NoSuchElementException exp) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    public  Response processIfProxy(final String[] replicaNodes,
                                    final String id,
                                    final List<RecordTimestamp> responses,
                                    final ByteBuffer key)
            throws InterruptedException, IOException, HttpException, PoolException {
        for (final String node : replicaNodes) {
            responses.addAll(processNode(node, id, key));
        }
        final RecordTimestamp mergedResponse = RecordTimestamp.mergeRecords(responses);
        if (mergedResponse.isValue()) {
            return new Response(Response.OK, mergedResponse.getValue().array());
        } else {
            return mergedResponceIsDeletedChecker(mergedResponse);
        }
    }

    public List<RecordTimestamp> processNode(final String node,
                                             final String id,
                                             final ByteBuffer key)
            throws InterruptedException, IOException, HttpException, PoolException {
        Response getResponse;
        List<RecordTimestamp> responses = new ArrayList<>();
        if (node.equals(nodes.getCurrentNodeId())) {
            getResponse = getMethodWrapper(key);

        } else {
            getResponse = clusterClients.get(node)
                    .get(URL + id, PROXY_HEADER);
        }
        if (getResponse.getStatus() == 404 && getResponse.getBody().length == 0) {
            responses.add(RecordTimestamp.getEmptyRecord());
        } else if (getResponse.getStatus() == 500) {
            return responses;
        } else {
            responses.add(RecordTimestamp.fromBytes(getResponse.getBody()));
        }
        return responses;
    }
}
