package ru.mail.polis.service.luckydaemon;

import com.google.common.base.Splitter;
import one.nio.http.HttpSession;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;

import static one.nio.http.Response.BAD_REQUEST;

public final class Replicas {
    private final int ack;
    private final int from;

    public Replicas(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    public int getFrom() {
        return from;
    }

    public int getAck() {
        return ack;
    }

    /**
     * Calculate the RF
     *
     * @param replics desired amount of replics
     * @param session current session
     * @param defaultRF if replics is null
     * @param clusterSize size of cluster
     * @return RF
     */
    public static Replicas calculateRF(final String replics,
                                       final int clusterSize,
                                       @NotNull final HttpSession session,
                                       final Replicas defaultRF) throws IOException {
        Replicas repl = null;
        try {
            repl = replics == null ? defaultRF : Replicas.of(replics);
            if (repl.ack < 1 || repl.from < repl.ack || repl.from > clusterSize) {
                throw new IllegalArgumentException("too big  from");
            }
            return repl;
        } catch (IllegalArgumentException e) {
            session.sendError(BAD_REQUEST, "wrong replica");
        }
        return repl;
    }

    @NotNull
    private static Replicas of(@NotNull final String value) {
        final String rem = value.replace("=", "");
        final List<String> values = Splitter.on('/').splitToList(rem);
        if (values.size() != 2) {
            throw new IllegalArgumentException("wrong replica factor");
        }
        return new Replicas(Integer.parseInt(values.get(0)), Integer.parseInt(values.get(1)));
    }
}
