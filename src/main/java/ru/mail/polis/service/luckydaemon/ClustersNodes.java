package ru.mail.polis.service.luckydaemon;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Collections;

public class ClustersNodes {

    private final List<String> nodesList;
    private final String id;

    /**
     * Constructor.
     *
     * @param nodes set of nodes.
     * @param id id.
     */
    public ClustersNodes(@NotNull final Set<String> nodes, @NotNull final String id) {
        final List<String> sortedNodes = new ArrayList<>(nodes);
        Collections.sort(sortedNodes);
        this.nodesList = sortedNodes;
        this.id = id;
    }

    /**
     * Check the key.
     *
     * @param key to search.
     * @return id of the cluster node.
     */
    public String keyCheck(@NotNull final ByteBuffer key) {
        final int hashCode = key.hashCode();
        final int node = (hashCode & Integer.MAX_VALUE) % nodesList.size();
        return nodesList.get(node);
    }

    public Set<String> getNodes() {
        return new HashSet<>(this.nodesList);
    }

    public String getCurrentNodeId() {
        return this.id;
    }

    public String[] getReplics(final int count, @NotNull final ByteBuffer key) {
        final String[] res = new String[count];
        int index = (key.hashCode() & Integer.MAX_VALUE) % nodesList.size();
        for (int i = 0; i < count; i++) {
            res[i] = nodesList.get(index);
            index = (index + 1) % nodesList.size();
        }
        return res;
    }
}
