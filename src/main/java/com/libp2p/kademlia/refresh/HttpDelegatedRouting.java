package com.libp2p.kademlia.refresh;

import com.libp2p.kademlia.protocol.KademliaProtocol;
import com.libp2p.kademlia.records.ProviderRecord;
import com.libp2p.kademlia.routing.DelegatedRouting;
import com.libp2p.kademlia.routing.KadPeer;
import com.libp2p.kademlia.routing.RoutingTable;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HTTP {@link DelegatedRouting} built on the public Delegated Routing V1
 * servers. For a given content key the servers resolve the {@code closest/peers}
 * deterministically, so every caller announces to and searches among the same
 * dialable peers — the provider records land where other callers look, even in
 * networks where a generic closest-peers kad crawl dead-ends on the key's
 * region.
 *
 * <p>Responsable addresses are cached briefly per key; dials and RPCs are
 * bounded so a dead or blocked server cannot stall callers.</p>
 */
public class HttpDelegatedRouting implements DelegatedRouting {

    private static final Duration RESPONSABLES_TTL = Duration.ofMinutes(5);
    private static final Duration RESPONSABLE_DIAL_TIMEOUT = Duration.ofSeconds(8);
    private static final long ANNOUNCE_SEND_TIMEOUT_MS = 3000;

    private final HttpBootstrapPeerSource source;
    private final KademliaProtocol protocol;
    private final RoutingTable routingTable;
    private final Duration bootstrapAddressTTL;
    private final java.util.Map<String, RespCacheEntry> responsablesCache = new ConcurrentHashMap<>();
    private volatile Host host;

    private record RespCacheEntry(List<Multiaddr> addrs, long expiresAtMillis) {}

    public HttpDelegatedRouting(HttpBootstrapPeerSource source, KademliaProtocol protocol,
                                RoutingTable routingTable, Duration bootstrapAddressTTL) {
        this.source = source;
        this.protocol = protocol;
        this.routingTable = routingTable;
        this.bootstrapAddressTTL = bootstrapAddressTTL;
    }

    /** Attach the host used to dial and announce; called on {@code KadDht.setHost()}. */
    public void setHost(Host host) {
        this.host = host;
    }

    @Override
    public CompletableFuture<Boolean> announce(byte[] key) {
        Host h = host;
        if (h == null) return CompletableFuture.completedFuture(false);
        return responsables(key).thenCompose(addrs -> {
            if (addrs.isEmpty()) return CompletableFuture.completedFuture(false);
            Set<PeerId> dialed = ConcurrentHashMap.newKeySet();
            List<CompletableFuture<Boolean>> futures = new ArrayList<>();
            for (Multiaddr m : addrs) {
                PeerId pid = m.getPeerId();
                if (pid == null || !dialed.add(pid)) continue;
                h.getAddressBook().addAddrs(pid, bootstrapAddressTTL.toMillis(), m);
                futures.add(h.getNetwork().connect(pid, m)
                        .orTimeout(RESPONSABLE_DIAL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                        .thenCompose(c -> protocol.sendAddProvider(key, pid))
                        .orTimeout(ANNOUNCE_SEND_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                        .exceptionally(ex -> false));
            }
            if (futures.isEmpty()) return CompletableFuture.completedFuture(false);
            return anySucceeded(futures);
        });
    }

    @Override
    public CompletableFuture<List<ProviderRecord>> findProviders(byte[] key) {
        Host h = host;
        if (h == null) return CompletableFuture.completedFuture(List.of());
        return responsables(key).thenCompose(addrs -> {
            Set<PeerId> dialed = ConcurrentHashMap.newKeySet();
            List<CompletableFuture<List<ProviderRecord>>> futures = new ArrayList<>();
            for (Multiaddr m : addrs) {
                PeerId pid = m.getPeerId();
                if (pid == null || !dialed.add(pid)) continue;
                h.getAddressBook().addAddrs(pid, bootstrapAddressTTL.toMillis(), m);
                futures.add(getProviders(h, key, pid, m));
            }
            if (futures.isEmpty()) return CompletableFuture.completedFuture(List.of());
            return firstNonEmpty(futures);
        });
    }

    /**
     * Fetches (with a short-lived cache) the dialable responsables for a content
     * key from the routing servers. Deterministic per key, so every caller
     * coordinates on the same candidate peers.
     */
    private CompletableFuture<List<Multiaddr>> responsables(byte[] key) {
        String cacheKey = java.util.Base64.getEncoder().encodeToString(key);
        RespCacheEntry entry = responsablesCache.get(cacheKey);
        if (entry != null && System.currentTimeMillis() < entry.expiresAtMillis()) {
            return CompletableFuture.completedFuture(entry.addrs());
        }
        return source.fetchForDhtKey(key).thenApply(addrs -> {
            responsablesCache.put(cacheKey, new RespCacheEntry(addrs, System.currentTimeMillis() + RESPONSABLES_TTL.toMillis()));
            return addrs;
        });
    }

    private CompletableFuture<List<ProviderRecord>> getProviders(Host h, byte[] key, PeerId pid, Multiaddr m) {
        return h.getNetwork().connect(pid, m)
                .thenCompose(c -> protocol.sendGetProviders(key, pid))
                .orTimeout(RESPONSABLE_DIAL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                .thenApply(r -> {
                    if (r.closerPeers() != null) {
                        for (KadPeer p : r.closerPeers()) routingTable.insert(p.nodeId, p.multiaddrs);
                    }
                    List<ProviderRecord> provs = new ArrayList<>(r.providers());
                    return provs;
                })
                .exceptionally(ex -> List.<ProviderRecord>of());
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

    /** Completes with the first non-empty result, else empty when all are done. */
    private static CompletableFuture<List<ProviderRecord>> firstNonEmpty(List<CompletableFuture<List<ProviderRecord>>> futures) {
        CompletableFuture<List<ProviderRecord>> result = new CompletableFuture<>();
        AtomicInteger remaining = new AtomicInteger(futures.size());
        for (CompletableFuture<List<ProviderRecord>> f : futures) {
            f.whenComplete((provs, ex) -> {
                if (result.isDone()) return;
                if (ex == null && provs != null && !provs.isEmpty()) {
                    result.complete(provs);
                } else if (remaining.decrementAndGet() == 0) {
                    result.complete(List.of());
                }
            });
        }
        return result;
    }
}