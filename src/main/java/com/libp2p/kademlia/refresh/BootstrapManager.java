package com.libp2p.kademlia.refresh;

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
    private volatile boolean bootstrapped = false;
    private volatile java.util.function.Function<byte[], CompletableFuture<Void>> findNodeFn;

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
        if (bootstrapped) return CompletableFuture.completedFuture(null);
        bootstrapped = true;
        CompletableFuture<Void> chain = connectBootstrapNodes()
                .thenCompose(v -> selfLookup())
                .thenCompose(v -> refreshBuckets())
                .thenApply(v -> null);
        if (queryTimeout != null) {
            chain = chain.orTimeout(queryTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
        return chain;
    }

    private CompletableFuture<Void> connectBootstrapNodes() {
        if (bootstrapNodes.isEmpty()) return CompletableFuture.completedFuture(null);
        if (host == null) return CompletableFuture.completedFuture(null);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Multiaddr addr : bootstrapNodes) {
            try {
                String addrStr = addr.toString();
                String[] parts = addrStr.split("/p2p/");
                if (parts.length > 1) {
                    PeerId peerId = PeerId.fromBase58(parts[1]);
                    host.getAddressBook().addAddrs(peerId, bootstrapAddressTTL, addr);
                    futures.add(host.newStream(List.of("/ipfs/ping/1.0.0"), peerId)
                            .getController()
                            .orTimeout(connectTimeout.toMillis(), TimeUnit.MILLISECONDS)
                            .thenAccept(ctrl -> routingTable.markSeen(peerId))
                            .exceptionally(ex -> null));
                }
            } catch (Exception ignored) {}
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
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
