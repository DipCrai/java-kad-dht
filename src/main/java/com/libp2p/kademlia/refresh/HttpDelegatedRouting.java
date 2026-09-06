package com.libp2p.kademlia.refresh;

import com.libp2p.kademlia.protocol.KademliaProtocol;
import com.libp2p.kademlia.records.ProviderRecord;
import com.libp2p.kademlia.routing.DelegatedRouting;
import com.libp2p.kademlia.routing.RoutingTable;
import io.libp2p.core.Host;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * HTTP {@link DelegatedRouting} built on the public Delegated Routing V1
 * servers. For a given content key the servers resolve the {@code closest/peers}
 * deterministically, so every caller announces to and searches among the same
 * dialable peers.
 *
 * <p>Responsable addresses are cached briefly per key with a <em>single-flight</em>
 * fetch, so concurrent callers for the same key share one in-flight router query
 * instead of stampeding the public server. The dial/RPC dogwork is delegated to
 * {@link ResponsablesRpc}; this class is only responsible for resolving,
 * caching and coordinating the responsable set.</p>
 */
public class HttpDelegatedRouting implements DelegatedRouting {

    private static final Duration RESPONSABLES_TTL = Duration.ofMinutes(5);

    private final PeerSource source;
    private final ResponsablesRpc rpc;
    private final java.util.Map<String, CompletableFuture<RespCacheEntry>> responsablesCache = new ConcurrentHashMap<>();
    private volatile Host host;

    private record RespCacheEntry(List<Multiaddr> addrs, long expiresAtMillis) {}

    public HttpDelegatedRouting(PeerSource source, KademliaProtocol protocol,
                                RoutingTable routingTable, Duration bootstrapAddressTTL) {
        this.source = source;
        this.rpc = new ResponsablesRpc(protocol, routingTable, bootstrapAddressTTL);
    }

    /** Attach the host used to dial and announce; called on {@code KadDht.setHost()}. */
    public void setHost(Host host) {
        this.host = host;
    }

    @Override
    public CompletableFuture<Boolean> announce(byte[] key) {
        Host h = host;
        if (h == null) return CompletableFuture.completedFuture(false);
        return responsables(key).thenCompose(addrs ->
                addrs.isEmpty() ? CompletableFuture.completedFuture(false) : rpc.announce(h, key, addrs));
    }

    @Override
    public CompletableFuture<List<ProviderRecord>> findProviders(byte[] key) {
        Host h = host;
        if (h == null) return CompletableFuture.completedFuture(List.of());
        return responsables(key).thenCompose(addrs ->
                addrs.isEmpty() ? CompletableFuture.completedFuture(List.of()) : rpc.findProviders(h, key, addrs));
    }

    /**
     * Single-flight, short-lived cache of the responsible peers for a content
     * key. All callers of the same key (even concurrent ones) share the same
     * router request; the cached address list is never aliased to callers.
     * Package-private for the cache-stampede and refetch tests.
     */
    CompletableFuture<List<Multiaddr>> responsables(byte[] key) {
        String cacheKey = java.util.Base64.getEncoder().encodeToString(key);
        CompletableFuture<RespCacheEntry> entryF = responsablesCache.compute(cacheKey, (k, existing) -> {
            if (existing != null && isFresh(existing)) return existing;
            return fetchEntry(key);
        });
        return entryF.thenApply(e -> List.copyOf(e.addrs()));
    }

    private CompletableFuture<RespCacheEntry> fetchEntry(byte[] key) {
        // a failed fetch installs an instantly-stale empty entry (never touches
        // the map from inside the compute remapping), so the next caller simply
        // refetches instead of being served a pinned failure
        return source.fetchForDhtKey(key)
                .thenApply(addrs -> new RespCacheEntry(List.copyOf(addrs),
                        System.currentTimeMillis() + RESPONSABLES_TTL.toMillis()))
                .exceptionally(ex -> new RespCacheEntry(List.of(), 0));
    }

    private static boolean isFresh(CompletableFuture<RespCacheEntry> f) {
        if (!f.isDone()) return true; // in-flight fetch is always fresh
        RespCacheEntry e = f.join();
        return e != null && e.expiresAtMillis() > System.currentTimeMillis();
    }
}