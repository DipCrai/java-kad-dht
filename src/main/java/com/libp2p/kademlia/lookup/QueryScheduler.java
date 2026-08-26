package com.libp2p.kademlia.lookup;

import io.libp2p.core.PeerId;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryScheduler {
    private final int maxInFlight;
    private final ConcurrentLinkedDeque<PeerId> pending = new ConcurrentLinkedDeque<>();
    private final Set<PeerId> inFlight = ConcurrentHashMap.newKeySet();
    private final Set<PeerId> completed = ConcurrentHashMap.newKeySet();
    private final IterativeLookup lookup;
    private final java.util.function.Function<PeerId, CompletableFuture<Void>> queryFunction;
    private final AtomicInteger activeCount = new AtomicInteger(0);
    private final CompletableFuture<Void> allDone = new CompletableFuture<>();
    private volatile boolean cancelled = false;
    private static final AtomicInteger globalActiveQueries = new AtomicInteger(0);
    private static volatile int globalMaxConcurrent = Integer.MAX_VALUE;

    public QueryScheduler(int maxInFlight, IterativeLookup lookup,
                          java.util.function.Function<PeerId, CompletableFuture<Void>> queryFunction,
                          Duration queryTimeout, ScheduledExecutorService scheduler) {
        this.maxInFlight = maxInFlight;
        this.lookup = lookup;
        this.queryFunction = queryFunction;
        lookup.getCancellation().whenComplete((v, ex) -> {
            cancel();
        });
        if (queryTimeout != null && scheduler != null) {
            scheduler.schedule(() -> {
                if (!allDone.isDone()) cancel();
            }, queryTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    public QueryScheduler(int maxInFlight, IterativeLookup lookup,
                          java.util.function.Function<PeerId, CompletableFuture<Void>> queryFunction) {
        this(maxInFlight, lookup, queryFunction, null, null);
    }

    public static void setGlobalMaxConcurrent(int max) {
        globalMaxConcurrent = max;
    }

    public static int getGlobalActiveCount() {
        return globalActiveQueries.get();
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
        while (activeCount.get() < maxInFlight && !pending.isEmpty()
                && globalActiveQueries.get() < globalMaxConcurrent) {
            PeerId next = pending.pollFirst();
            if (next == null || completed.contains(next) || inFlight.contains(next)) continue;
            inFlight.add(next);
            activeCount.incrementAndGet();
            globalActiveQueries.incrementAndGet();
            CompletableFuture<Void> future = queryFunction.apply(next);
            future.whenComplete((v, ex) -> {
                inFlight.remove(next);
                completed.add(next);
                activeCount.decrementAndGet();
                globalActiveQueries.decrementAndGet();
                dispatch();
            });
        }
        if (pending.isEmpty() && activeCount.get() == 0 && !lookup.isFinished()) {
            lookup.getCancellation().complete(null);
            checkAllDone();
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
