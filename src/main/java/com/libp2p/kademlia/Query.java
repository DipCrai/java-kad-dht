package com.libp2p.kademlia;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A single query with its state, iterator, and result.
 * Port of rust Query struct.
 */
public class Query {
    public final String id;
    public final QueryInfo info;
    public final ClosestPeersIter iterator;
    public final QueryStats stats;
    public final Instant createdAt;
    public final Duration timeout;
    private final CompletableFuture<List<KadPeer>> future;

    public Query(QueryInfo info, ClosestPeersIter iterator, Duration timeout) {
        this.id = UUID.randomUUID().toString().substring(0, 8);
        this.info = info;
        this.iterator = iterator;
        this.stats = new QueryStats();
        this.createdAt = Instant.now();
        this.timeout = timeout;
        this.future = new CompletableFuture<>();
    }

    public boolean isTimedOut() {
        return Instant.now().isAfter(createdAt.plus(timeout));
    }

    public boolean isFinished() {
        return iterator.isFinished() || isTimedOut();
    }

    public CompletableFuture<List<KadPeer>> getFuture() {
        return future;
    }

    public void complete() {
        if (!future.isDone()) {
            future.complete(iterator.getResult());
        }
    }

    public void completeExceptionally(Throwable t) {
        if (!future.isDone()) {
            future.completeExceptionally(t);
        }
    }

    @Override
    public String toString() {
        return "Query{id=" + id + ", type=" + info.type + ", state=" + iterator.getState() +
                ", results=" + iterator.getResultCount() + ", stats=" + stats + "}";
    }
}
