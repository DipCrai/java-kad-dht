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
    private volatile boolean bootstrapped = false;

    public BootstrapManager(RoutingTable routingTable, Host host, List<Multiaddr> bootstrapNodes, Duration connectTimeout) {
        this.routingTable = routingTable;
        this.host = host;
        this.bootstrapNodes = bootstrapNodes;
        this.connectTimeout = connectTimeout;
    }

    public void setHost(Host host) { this.host = host; }

    public CompletableFuture<Void> bootstrap() {
        if (bootstrapped) return CompletableFuture.completedFuture(null);
        bootstrapped = true;
        return connectBootstrapNodes()
                .thenCompose(v -> selfLookup())
                .thenCompose(v -> refreshBuckets())
                .thenApply(v -> null);
    }

    private CompletableFuture<Void> connectBootstrapNodes() {
        if (bootstrapNodes.isEmpty()) return CompletableFuture.completedFuture(null);
        for (Multiaddr addr : bootstrapNodes) {
            try {
                String addrStr = addr.toString();
                String[] parts = addrStr.split("/p2p/");
                if (parts.length > 1) {
                    PeerId peerId = PeerId.fromBase58(parts[1]);
                    routingTable.markSeen(peerId);
                }
            } catch (Exception ignored) {}
        }
        return CompletableFuture.completedFuture(null);
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
        for (int i = 1; i < 256; i++) {
            if (routingTable.getBucket(i).size() > 0) {
                byte[] randomKey = XorId.generateRandomKeyForBucket(selfKey, i);
                futures.add(iterativeFindNode(randomKey));
            }
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    private CompletableFuture<Void> iterativeFindNode(byte[] target) {
        return CompletableFuture.supplyAsync(() -> {
            List<KadPeer> active = getActivePeers();
            if (active.isEmpty()) return null;

            Set<PeerId> queried = ConcurrentHashMap.newKeySet();
            List<KadPeer> candidates = new ArrayList<>(active);
            int noProgress = 0;

            while (noProgress < 3) {
                List<PeerId> toQuery = new ArrayList<>();
                for (KadPeer p : candidates) {
                    if (!queried.contains(p.nodeId) && toQuery.size() < 3) {
                        toQuery.add(p.nodeId);
                        queried.add(p.nodeId);
                    }
                }
                if (toQuery.isEmpty()) break;

                List<CompletableFuture<List<KadPeer>>> futures = new ArrayList<>();
                for (PeerId peer : toQuery) {
                    futures.add(CompletableFuture.supplyAsync(() -> {
                        try {
                            var msg = com.libp2p.kademlia.protocol.RpcCodec.findNode(target);
                            var ctrl = (com.libp2p.kademlia.protocol.KademliaProtocol.KademliaController) host.newStream(List.of("/ipfs/kad/1.0.0"), peer)
                                    .getController().get(connectTimeout.toSeconds(), TimeUnit.SECONDS);
                            var resp = ctrl.sendRequest(msg)
                                    .get(connectTimeout.toSeconds(), TimeUnit.SECONDS);
                            return parseCloserPeers(resp);
                        } catch (Exception e) {
                            return List.<KadPeer>of();
                        }
                    }));
                }

                boolean progress = false;
                for (CompletableFuture<List<KadPeer>> f : futures) {
                    try {
                        List<KadPeer> closer = f.get(connectTimeout.toSeconds(), TimeUnit.SECONDS);
                        for (KadPeer p : closer) {
                            if (!containsPeer(candidates, p.nodeId)) {
                                candidates.add(p);
                                routingTable.insert(p.nodeId, p.multiaddrs);
                                progress = true;
                            }
                        }
                    } catch (Exception ignored) {}
                }
                noProgress = progress ? 0 : noProgress + 1;
            }
            return null;
        }, Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "bootstrap");
            t.setDaemon(true);
            return t;
        }));
    }

    private List<KadPeer> getActivePeers() {
        List<KadPeer> peers = new ArrayList<>();
        for (PeerId peer : routingTable.getAllPeers()) {
            List<Multiaddr> addrs;
            try { addrs = new ArrayList<>(host.getAddressBook().getAddrs(peer).get(2, TimeUnit.SECONDS)); }
            catch (Exception e) { addrs = List.of(); }
            peers.add(new KadPeer(peer, addrs, KadPeer.ConnectionType.CONNECTED));
        }
        return peers;
    }

    private boolean containsPeer(List<KadPeer> peers, PeerId id) {
        return peers.stream().anyMatch(p -> p.nodeId.equals(id));
    }

    private List<KadPeer> parseCloserPeers(com.libp2p.kademlia.pb.Dht.Message msg) {
        List<KadPeer> peers = new ArrayList<>();
        for (var p : msg.getCloserPeersList()) {
            try {
                PeerId nodeId = new PeerId(p.getId().toByteArray());
                List<Multiaddr> addrs = new ArrayList<>();
                for (var ab : p.getAddrsList()) {
                    try { addrs.add(Multiaddr.deserialize(ab.toByteArray())); } catch (Exception ignored) {}
                }
                peers.add(new KadPeer(nodeId, addrs, KadPeer.ConnectionType.fromValue(p.getConnection().getNumber())));
            } catch (Exception ignored) {}
        }
        return peers;
    }
}
