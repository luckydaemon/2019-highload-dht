package ru.mail.polis.service.luckydaemon;

import java.net.URI;
import java.net.http.HttpRequest;
import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;



public class HtttRequestBuilder {
    private static final String PROXY_HEADER = "PROXY_HEADER";
    private static final String URL = "/v0/entity?id=";

    public static HttpRequest getUpsertHttpRequest(final String node, final String id, final byte[] value) {
        final HttpRequest request = requestTemplate(node,id).PUT(HttpRequest.BodyPublishers.ofByteArray(value)).build();
        return request;
    }

    public static HttpRequest getDeleteHttpRequest(final String node, final String id) {
        final HttpRequest request = requestTemplate(node,id).DELETE().build();
        return request;
    }

    public static HttpRequest getHttpRequest(final String node, final String id) {
        final HttpRequest request = requestTemplate(node,id).GET().build();
        return request;
    }

   private static HttpRequest.Builder requestTemplate(final String node, final String id) {
        return HttpRequest.newBuilder()
                .uri(URI.create(node + URL + id))
                .timeout(Duration.of(5, SECONDS))
                .setHeader(PROXY_HEADER, "True");
    }
}
