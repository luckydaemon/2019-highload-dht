package ru.mail.polis.service.luckydaemon;

import java.net.URI;
import java.net.http.HttpRequest;
import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;

public final class HtttRequestBuilder {
    private static final String PROXY_HEADER = "PROXY_HEADER";
    private static final String URL = "/v0/entity?id=";

    private  HtttRequestBuilder(){
    }

    public static HttpRequest createPutHttpRequest(final String node, final String id, final byte[] value) {
        return requestTemplate(node,id).PUT(HttpRequest.BodyPublishers.ofByteArray(value)).build();
    }

    public static HttpRequest createDeleteHttpRequest(final String node, final String id) {
        return requestTemplate(node,id).DELETE().build();
    }

    public static HttpRequest createGetHttpRequest(final String node, final String id) {
        return requestTemplate(node,id).GET().build();
    }

   private static HttpRequest.Builder requestTemplate(final String node, final String id) {
        return HttpRequest.newBuilder()
                .uri(URI.create(node + URL + id))
                .timeout(Duration.of(5, SECONDS))
                .setHeader(PROXY_HEADER, "True");
    }
}
