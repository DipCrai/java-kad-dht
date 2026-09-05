package com.libp2p.kademlia.refresh;

import com.libp2p.kademlia.DnsaddrResolver;
import com.libp2p.kademlia.routing.RoutingTable;
import com.libp2p.kademlia.routing.KadPeer;
import com.libp2p.kademlia.XorId;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class BootstrapManager {
    private final RoutingTable routingTable;
    private volatile Host host;
    private final List<Multiaddr> bootstrapNodes;
    private final Duration connectTimeout;
    private final Duration queryTimeout;
    private final long bootstrapAddressTTL;
    private volatile BootstrapState state = BootstrapState.NOT_STARTED;
    private volatile CompletableFuture<Void> bootstrapFuture;
    private volatile java.util.function.Function<byte[], CompletableFuture<Void>> findNodeFn;

    public enum BootstrapState { NOT_STARTED, RUNNING, SUCCEEDED, FAILED }

    public BootstrapManager(RoutingTable routingTable, Host host, List<Multiaddr> bootstrapNodes, Duration connectTimeout, Duration queryTimeout, long bootstrapAddressTTL) {
        this.routingTable = routingTable;
        this.host = host;
        this.bootstrapNodes = bootstrapNodes;
        this.connectTimeout = connectTimeout;
        this.queryTimeout = queryTimeout;
        this.bootstrapAddressTTL = bootstrapAddressTTL;
    }

    public void setHost(Host host) { this.host = host; }
    public void setFindNodeFn(java.util.function.Function<byte[], CompletableFuture<Void>> fn) { this.findNodeFn = fn; }

    public CompletableFuture<Void> bootstrap() {
        if (state == BootstrapState.RUNNING && bootstrapFuture != null) {
            return bootstrapFuture;
        }
        if (state == BootstrapState.SUCCEEDED) {
            return CompletableFuture.completedFuture(null);
        }
        state = BootstrapState.RUNNING;
        CompletableFuture<Void> chain = connectBootstrapNodes()
                .thenCompose(v -> selfLookup())
                .thenCompose(v -> refreshBuckets())
                .thenApply(v -> null);
        if (queryTimeout != null) {
            chain = chain.orTimeout(queryTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
        bootstrapFuture = chain.whenComplete((v, ex) -> {
            state = ex == null ? BootstrapState.SUCCEEDED : BootstrapState.FAILED;
            bootstrapFuture = null;
        });
        return bootstrapFuture;
    }

    private CompletableFuture<Void> connectBootstrapNodes() {
        if (bootstrapNodes.isEmpty()) return CompletableFuture.completedFuture(null);
        if (host == null) return CompletableFuture.completedFuture(null);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Multiaddr addr : expandedBootstrapAddrs()) {
            try {
                String addrStr = addr.toString();
                String[] parts = addrStr.split("/p2p/");
                if (parts.length > 1) {
                    PeerId peerId = PeerId.fromBase58(parts[1]);
                    host.getAddressBook().addAddrs(peerId, bootstrapAddressTTL, addr);
                    futures.add(host.getNetwork().connect(peerId, addr)
                            .orTimeout(connectTimeout.toMillis(), TimeUnit.MILLISECONDS)
                            .thenAccept(conn -> {
                                routingTable.insert(peerId, List.of(addr));
                            })
                            .exceptionally(ex -> null));
                }
            } catch (Exception ignored) {}
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    /**
     * Resolves DNS-based bootstrap multiaddrs ({@code /dnsaddr/...}, {@code /dns4/...},
     * {@code /dns6/...}, {@code /dns/...}) into concrete {@code /ip4|/ip6} addresses on
     * every bootstrap round, staying in sync with DNS changes the same way go-libp2p does.
     */
    private List<Multiaddr> expandedBootstrapAddrs() {
        List<Multiaddr> out = new ArrayList<>();
        for (Multiaddr addr : bootstrapNodes) {
            String as = addr.toString();
            if (DnsaddrResolver.isResolvable(as)) {
                out.addAll(DnsaddrResolver.resolve(as));
            } else {
                out.add(addr);
            }
        }
        return out;
    }

    private CompletableFuture<Void> selfLookup() {
        if (host == null) return CompletableFuture.completedFuture(null);
        byte[] selfKey = XorId.fromPeerId(host.getPeerId());
        return iterativeFindNode(selfKey);
    }

    private CompletableFuture<Void> refreshBuckets() {
        if (host == null) return CompletableFuture.completedFuture(null);
        byte[] selfKey = XorId.fromPeerId(host.getPeerId());
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 256; i++) {
            if (routingTable.getBucket(i).size() > 0) {
                byte[] randomKey = XorId.generateRandomKeyForBucket(selfKey, i);
                futures.add(iterativeFindNode(randomKey));
            }
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    private CompletableFuture<Void> iterativeFindNode(byte[] target) {
        if (findNodeFn == null) throw new IllegalStateException("findNodeFn not set");
        return findNodeFn.apply(target);
    }
}
