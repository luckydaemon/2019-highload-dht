package ru.mail.polis.service.luckydaemon;

import one.nio.http.Response;

import java.net.http.HttpResponse;

public final class DaoMethodsHandlers {
    private static final int ACCEPTED = new Response(Response.ACCEPTED).getStatus();
    private static final int CREATED = new Response(Response.CREATED).getStatus();

    private DaoMethodsHandlers(){}

    public static String daoRemoveFutureHandler(final Throwable throwable) {
        if (throwable != null) {
            return Response.INTERNAL_ERROR;
        }
        return Response.ACCEPTED;
    }

    public static String daoRemoveFutureHandlerWithResponse(final HttpResponse<Void> response) {
        if (response.statusCode() == ACCEPTED) {
            return Response.ACCEPTED;
        }
        return Response.INTERNAL_ERROR;
    }

    public static String daoUpsertFuturehandler(final Throwable throwable) {
        if (throwable != null) {
            return Response.INTERNAL_ERROR;
        }
        return Response.CREATED;
    }

    public static String daoUpsertFuturehandlerWithResponse(final HttpResponse<Void> response) {
        if (response.statusCode() == CREATED) {
            return Response.CREATED;
        }
        return Response.INTERNAL_ERROR;
    }
}
