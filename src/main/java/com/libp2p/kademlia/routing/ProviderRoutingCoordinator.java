package com.libp2p.kademlia.routing;

import com.libp2p.kademlia.records.ProviderRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Runs the provider operations through the native kad path and an optional
 * {@link DelegatedRouting} path (e.g. HTTP responsables), so {@code KadDht}
 * itself never has to know about the delegated implementation.
 *
 * <p><b>provide(key)</b>: both paths start in parallel. The future completes
 * {@code true} as soon as either path confirms the announce (the kad path keeps
 * replicating in the background once the delegated path wins); it completes
 * {@code false} only when both paths finished without success. When no
 * delegated path is configured, the native kad result is returned unchanged.
 *
 * <p><b>findProviders(key)</b>: both paths start in parallel. The future
 * returns the first non-empty answer, but provider records arriving from the
 * other path within a short grace window are merged and deduplicated — the
 * window is deadline-based (a timer), never a blocking sleep.
 */
public class ProviderRoutingCoordinator {

    private static final Duration DEFAULT_KAD_PROVIDE_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration DEFAULT_KAD_FIND_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration DEFAULT_MERGE_GRACE = Duration.ofMillis(250);

    /** Daemon timer backing the merge grace windows; never keeps the JVM alive. */
    private static final ScheduledExecutorService GRACE = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "kad-provider-grace");
        t.setDaemon(true);
        return t;
    });

    private final Function<byte[], CompletableFuture<Boolean>> kadProvide;
    private final Function<byte[], CompletableFuture<List<ProviderRecord>>> kadFindProviders;
    private final DelegatedRouting delegated;
    private final Duration kadProvideTimeout;
    private final Duration kadFindTimeout;
    private final Duration mergeGrace;

    public ProviderRoutingCoordinator(
            Function<byte[], CompletableFuture<Boolean>> kadProvide,
            Function<byte[], CompletableFuture<List<ProviderRecord>>> kadFindProviders,
            DelegatedRouting delegated) {
        this(kadProvide, kadFindProviders, delegated, DEFAULT_KAD_PROVIDE_TIMEOUT, DEFAULT_KAD_FIND_TIMEOUT, DEFAULT_MERGE_GRACE);
    }

    public ProviderRoutingCoordinator(
            Function<byte[], CompletableFuture<Boolean>> kadProvide,
            Function<byte[], CompletableFuture<List<ProviderRecord>>> kadFindProviders,
            DelegatedRouting delegated,
            Duration kadProvideTimeout,
            Duration kadFindTimeout,
            Duration mergeGrace) {
        this.kadProvide = kadProvide;
        this.kadFindProviders = kadFindProviders;
        this.delegated = delegated;
        this.kadProvideTimeout = kadProvideTimeout;
        this.kadFindTimeout = kadFindTimeout;
        this.mergeGrace = mergeGrace;
    }

    /** @param key the content key */
    public CompletableFuture<Boolean> provide(byte[] key) {
        CompletableFuture<Boolean> kad = kadProvide.apply(key)
                .orTimeout(kadProvideTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .exceptionally(ex -> false);
        if (delegated == null) return kad;
        CompletableFuture<Boolean> http = delegated.announce(key).exceptionally(ex -> false);
        return anySucceeded(List.of(kad, http));
    }

    /** @param key the content key */
    public CompletableFuture<List<ProviderRecord>> findProviders(byte[] key) {
        CompletableFuture<List<ProviderRecord>> kad = kadFindProviders.apply(key)
                .orTimeout(kadFindTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .exceptionally(ex -> List.of());
        if (delegated == null) return kad;
        return firstNonEmptyThenMerge(kad, delegated.findProviders(key));
    }

    /** Completes true as soon as one future reports success, else false when all are done. */
    private static CompletableFuture<Boolean> anySucceeded(List<CompletableFuture<Boolean>> futures) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        AtomicInteger remaining = new AtomicInteger(futures.size());
        for (CompletableFuture<Boolean> f : futures) {
            f.whenComplete((ok, ex) -> {
                if (result.isDone()) return;
                if (Boolean.TRUE.equals(ok)) result.complete(true);
                else if (remaining.decrementAndGet() == 0) result.complete(false);
            });
        }
        return result;
    }

    /**
     * Returns the first non-empty answer, keeping a short deadline window for
     * the other path to land before the merge is committed (deduplicating by
     * provider peer id). If both paths answer empty, returns empty.
     */
    private CompletableFuture<List<ProviderRecord>> firstNonEmptyThenMerge(
            CompletableFuture<List<ProviderRecord>> a, CompletableFuture<List<ProviderRecord>> b) {
        CompletableFuture<List<ProviderRecord>> result = new CompletableFuture<>();
        GraceMerge merge = new GraceMerge(result, mergeGrace);
        a.whenComplete(merge::accept);
        b.whenComplete(merge::accept);
        return result;
    }

    private static List<ProviderRecord> mergeProviders(List<ProviderRecord> a, List<ProviderRecord> b) {
        List<ProviderRecord> merged = new ArrayList<>(a);
        if (b != null) {
            for (ProviderRecord pr : b) {
                if (!merged.stream().anyMatch(p -> p.getProvider().equals(pr.getProvider()))) merged.add(pr);
            }
        }
        return merged;
    }

    /** Deadline-based merge of the two racing provider paths. */
    private static final class GraceMerge {

        private final CompletableFuture<List<ProviderRecord>> target;
        private final long graceMillis;
        private final Object lock = new Object();
        private List<ProviderRecord> firstNonEmpty;
        private List<ProviderRecord> tail;
        private int pending = 2;
        private boolean finished;
        private ScheduledFuture<?> timer;

        GraceMerge(CompletableFuture<List<ProviderRecord>> target, Duration grace) {
            this.target = target;
            this.graceMillis = grace.toMillis();
        }

        void accept(List<ProviderRecord> provs, Throwable ex) {
            List<ProviderRecord> v = (ex == null && provs != null) ? provs : List.of();
            List<ProviderRecord> toComplete = null;
            synchronized (lock) {
                if (finished) return;
                pending--;
                if (!v.isEmpty()) {
                    if (firstNonEmpty == null) {
                        firstNonEmpty = v;
                        if (timer == null && pending > 0) {
                            timer = GRACE.schedule(this::onGrace, graceMillis, TimeUnit.MILLISECONDS);
                        }
                    } else {
                        tail = v;
                    }
                }
                if (pending == 0 && !finished) {
                    finished = true;
                    if (timer != null) timer.cancel(false);
                    toComplete = firstNonEmpty == null ? List.of() : mergeProviders(firstNonEmpty, tail);
                }
            }
            if (toComplete != null) target.complete(toComplete);
        }

        private void onGrace() {
            List<ProviderRecord> toComplete;
            synchronized (lock) {
                if (finished) return;
                finished = true;
                toComplete = firstNonEmpty == null ? List.of() : mergeProviders(firstNonEmpty, tail);
            }
            target.complete(toComplete);
        }
    }
}