package ru.mail.polis.service.luckydaemon;

import one.nio.http.Response;

import java.net.http.HttpResponse;

public final class DaoMethodsHandlers {
    private static final int ACCEPTED = new Response(Response.ACCEPTED).getStatus();
    private static final int CREATED = new Response(Response.CREATED).getStatus();

    private DaoMethodsHandlers(){
    }

    /**
     * Error handler in delete method.
     *
     * @param throwable - error object
     * @return response
     */
    public static String daoRemoveFutureHandler(final Throwable throwable) {
        if (throwable != null) {
            return Response.INTERNAL_ERROR;
        }
        return Response.ACCEPTED;
    }

    /**
     * Response wrapper in delete method.
     *
     * @param response - java.net http response
     * @return one.nio http response
     */
    public static String daoRemoveFutureHandlerWithResponse(final HttpResponse<Void> response) {
        if (response.statusCode() == ACCEPTED) {
            return Response.ACCEPTED;
        }
        return Response.INTERNAL_ERROR;
    }

    /**
     * Error handler in upsert method.
     *
     * @param throwable - error object
     * @return response
     */
    public static String daoUpsertFuturehandler(final Throwable throwable) {
        if (throwable != null) {
            return Response.INTERNAL_ERROR;
        }
        return Response.CREATED;
    }

    /**
     * Response wrapper in upsert method.
     *
     * @param response - java.net http response
     * @return one.nio http response
     */
    public static String daoUpsertFuturehandlerWithResponse(final HttpResponse<Void> response) {
        if (response.statusCode() == CREATED) {
            return Response.CREATED;
        }
        return Response.INTERNAL_ERROR;
    }
}
