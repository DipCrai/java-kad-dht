package com.libp2p.kademlia;

import io.libp2p.core.PeerId;

import java.util.*;

/**
 * Manages all active queries.
 * Port of rust QueryPool.
 */
public class QueryPool {
    private final Map<String, Query> queries = new LinkedHashMap<>();

    public Query addQuery(Query query) {
        queries.put(query.id, query);
        return query;
    }

    /**
     * Poll all queries and return actions to take.
     */
    public List<QueryAction> poll() {
        List<QueryAction> actions = new ArrayList<>();

        Iterator<Map.Entry<String, Query>> it = queries.entrySet().iterator();
        while (it.hasNext()) {
            Query query = it.next().getValue();

            if (query.isTimedOut()) {
                query.completeExceptionally(new java.util.concurrent.TimeoutException("Query timed out: " + query.id));
                it.remove();
                actions.add(new QueryAction(QueryAction.Type.TIMEOUT, query, null));
                continue;
            }

            if (query.isFinished()) {
                query.complete();
                it.remove();
                actions.add(new QueryAction(QueryAction.Type.FINISHED, query, null));
                continue;
            }

            PeerId next = query.iterator.next();
            if (next != null) {
                actions.add(new QueryAction(QueryAction.Type.WAITING, query, next));
            }
        }

        return actions;
    }

    public Query getQuery(String id) {
        return queries.get(id);
    }

    public int activeCount() {
        return queries.size();
    }

    public void cancel(String queryId) {
        Query q = queries.remove(queryId);
        if (q != null) {
            q.completeExceptionally(new InterruptedException("Query cancelled"));
        }
    }

    public static class QueryAction {
        public enum Type {
            WAITING,
            FINISHED,
            TIMEOUT
        }

        public final Type type;
        public final Query query;
        public final PeerId peer;

        public QueryAction(Type type, Query query, PeerId peer) {
            this.type = type;
            this.query = query;
            this.peer = peer;
        }
    }
}
