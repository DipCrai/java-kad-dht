package com.libp2p.kademlia.lookup;

import io.libp2p.core.PeerId;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryScheduler {
    private final int maxInFlight;
    private final ConcurrentLinkedDeque<PeerId> pending = new ConcurrentLinkedDeque<>();
    private final ConcurrentSkipListSet<PeerId> inFlight = new ConcurrentSkipListSet<>();
    private final ConcurrentSkipListSet<PeerId> completed = new ConcurrentSkipListSet<>();
    private final IterativeLookup lookup;
    private final java.util.function.Function<PeerId, CompletableFuture<Void>> queryFunction;
    private final AtomicInteger activeCount = new AtomicInteger(0);
    private final CompletableFuture<Void> allDone = new CompletableFuture<>();
    private volatile boolean cancelled = false;

    public QueryScheduler(int maxInFlight, IterativeLookup lookup,
                          java.util.function.Function<PeerId, CompletableFuture<Void>> queryFunction) {
        this.maxInFlight = maxInFlight;
        this.lookup = lookup;
        this.queryFunction = queryFunction;
        lookup.getCancellation().whenComplete((v, ex) -> {
            cancel();
        });
    }

    public void submitPeers(java.util.List<PeerId> peers) {
        for (PeerId p : peers) {
            if (!inFlight.contains(p) && !completed.contains(p)) {
                pending.addLast(p);
            }
        }
        dispatch();
    }

    private synchronized void dispatch() {
        if (cancelled || lookup.isFinished()) {
            checkAllDone();
            return;
        }
        while (activeCount.get() < maxInFlight && !pending.isEmpty()) {
            PeerId next = pending.pollFirst();
            if (next == null || completed.contains(next) || inFlight.contains(next)) continue;
            inFlight.add(next);
            activeCount.incrementAndGet();
            CompletableFuture<Void> future = queryFunction.apply(next);
            future.whenComplete((v, ex) -> {
                inFlight.remove(next);
                completed.add(next);
                activeCount.decrementAndGet();
                dispatch();
            });
        }
        if (pending.isEmpty() && activeCount.get() == 0 && !lookup.isFinished()) {
            dispatch();
            if (activeCount.get() == 0 && pending.isEmpty()) {
                checkAllDone();
            }
        }
    }

    private void checkAllDone() {
        if (activeCount.get() == 0 && pending.isEmpty()) {
            allDone.complete(null);
        }
    }

    public CompletableFuture<Void> awaitCompletion() {
        dispatch();
        return allDone;
    }

    public void cancel() {
        cancelled = true;
        allDone.complete(null);
    }
}
