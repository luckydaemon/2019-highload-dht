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
     * Calculate the RF.
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
        Replicas replica = null;
        try {
            if (replics == null) {
                replica = defaultRF;
            } else {
                replica = Replicas.of(replics);
            }
            if (replica.ack < 1 || replica.from < replica.ack || replica.from > clusterSize) {
                throw new IllegalArgumentException("invalid value of rf");
            }
            return replica;
        } catch (IllegalArgumentException e) {
            session.sendError(BAD_REQUEST, "wrong replica");
        }
        return replica;
    }

    /**
     * Get correct rf instead of default.
     * @param value - value of rf
     */
    @NotNull
    public static Replicas of(@NotNull final String value) {
        final String rem = value.replace("=", "");
        final List<String> values = Splitter.on('/').splitToList(rem);
        if (values.size() != 2) {
            throw new IllegalArgumentException("wrong replica factor");
        }
        return new Replicas(Integer.parseInt(values.get(0)), Integer.parseInt(values.get(1)));
    }
}
