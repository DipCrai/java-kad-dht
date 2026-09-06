package com.libp2p.kademlia.refresh;

import com.libp2p.kademlia.protocol.KademliaProtocol;
import com.libp2p.kademlia.records.ProviderRecord;
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
 * Low-level kad RPC layer over the responsible peers resolved for a content
 * key: dials each responsable, sends ADD_PROVIDER / GET_PROVIDERS, feeds the
 * closer peers back through the routing table's single admission point
 * ({@link RoutingTable#insertDiscovered}) and bounds every step so a dead or
 * blocked responsable cannot stall its caller.
 */
final class ResponsablesRpc {

    private static final Duration DIAL_TIMEOUT = Duration.ofSeconds(8);
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(4);

    private final KademliaProtocol protocol;
    private final RoutingTable routingTable;
    private final Duration bootstrapAddressTTL;

    ResponsablesRpc(KademliaProtocol protocol, RoutingTable routingTable, Duration bootstrapAddressTTL) {
        this.protocol = protocol;
        this.routingTable = routingTable;
        this.bootstrapAddressTTL = bootstrapAddressTTL;
    }

    /** Announces to every responsable; true as soon as one confirmed. */
    CompletableFuture<Boolean> announce(Host h, byte[] key, List<Multiaddr> addrs) {
        Set<PeerId> dialed = ConcurrentHashMap.newKeySet();
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        for (Multiaddr m : addrs) {
            PeerId pid = m.getPeerId();
            if (pid == null || !dialed.add(pid)) continue;
            h.getAddressBook().addAddrs(pid, bootstrapAddressTTL.toMillis(), m);
            futures.add(h.getNetwork().connect(pid, m)
                    .orTimeout(DIAL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .thenCompose(c -> protocol.sendAddProvider(key, pid))
                    .orTimeout(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .exceptionally(ex -> false));
        }
        if (futures.isEmpty()) return CompletableFuture.completedFuture(false);
        return anySucceeded(futures);
    }

    /** GET_PROVIDERS from every responsable, returning the first non-empty answer. */
    CompletableFuture<List<ProviderRecord>> findProviders(Host h, byte[] key, List<Multiaddr> addrs) {
        Set<PeerId> dialed = ConcurrentHashMap.newKeySet();
        List<CompletableFuture<List<ProviderRecord>>> futures = new ArrayList<>();
        for (Multiaddr m : addrs) {
            PeerId pid = m.getPeerId();
            if (pid == null || !dialed.add(pid)) continue;
            h.getAddressBook().addAddrs(pid, bootstrapAddressTTL.toMillis(), m);
            futures.add(h.getNetwork().connect(pid, m)
                    .orTimeout(DIAL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                    .thenCompose(c -> protocol.sendGetProviders(key, pid))
                    .thenApply(r -> providersFrom(r))
                    .exceptionally(ex -> List.of()));
        }
        if (futures.isEmpty()) return CompletableFuture.completedFuture(List.of());
        return firstNonEmpty(futures);
    }

    private List<ProviderRecord> providersFrom(com.libp2p.kademlia.protocol.KademliaProtocol.GetProvidersResponse r) {
        // closer peers from the delegated backend go through the routing table's
        // single admission point shared with kad-native discovery
        if (r.closerPeers() != null) {
            for (KadPeer p : r.closerPeers()) {
                routingTable.insertDiscovered(p.nodeId, p.multiaddrs);
            }
        }
        return new ArrayList<>(r.providers());
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