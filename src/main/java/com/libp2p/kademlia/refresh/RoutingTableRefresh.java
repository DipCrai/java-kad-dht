package com.libp2p.kademlia.refresh;

import com.libp2p.kademlia.protocol.KademliaProtocol;
import com.libp2p.kademlia.routing.RoutingTable;
import com.libp2p.kademlia.routing.KadPeer;
import com.libp2p.kademlia.XorId;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class RoutingTableRefresh {
    private final RoutingTable routingTable;
    private volatile Host host;
    private volatile KademliaProtocol protocol;
    private final Duration refreshInterval;
    private final Duration peerTimeout;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> task;

    public RoutingTableRefresh(RoutingTable routingTable, Host host, Duration refreshInterval, Duration peerTimeout) {
        this.routingTable = routingTable;
        this.host = host;
        this.refreshInterval = refreshInterval;
        this.peerTimeout = peerTimeout;
    }

    public void setHost(Host host) { this.host = host; }
    public void setProtocol(KademliaProtocol protocol) { this.protocol = protocol; }

    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rt-refresh");
            t.setDaemon(true);
            return t;
        });
        task = scheduler.scheduleWithFixedDelay(this::refresh, 0, refreshInterval.toSeconds(), TimeUnit.SECONDS);
    }

    public void stop() {
        if (task != null) task.cancel(false);
        if (scheduler != null) scheduler.shutdownNow();
    }

    private void refresh() {
        try {
            if (host == null || protocol == null) return;
            byte[] selfKey = XorId.fromPeerId(host.getPeerId());
            List<Integer> buckets = routingTable.getNonEmptyBucketIndices();
            for (int idx : buckets) {
                byte[] randomKey;
                if (idx == 0) {
                    randomKey = selfKey.clone();
                    randomKey[0] = (byte) (randomKey[0] ^ (byte) 0x80);
                } else {
                    randomKey = XorId.generateRandomKeyForBucket(selfKey, idx);
                }
                iterativeFindNode(randomKey);
            }
        } catch (Exception ignored) {}
    }

    private void iterativeFindNode(byte[] target) {
        try {
            List<KadPeer> active = getActivePeers();
            if (active.isEmpty()) return;

            Set<PeerId> queried = ConcurrentHashMap.newKeySet();
            List<KadPeer> candidates = new ArrayList<>(active);
            int noProgress = 0;
            int concurrency = 3;
            ExecutorService exec = Executors.newFixedThreadPool(concurrency, r -> {
                Thread t = new Thread(r, "rt-refresh-fan");
                t.setDaemon(true);
                return t;
            });

            try {
                while (noProgress < 3) {
                    List<KadPeer> toQuery = new ArrayList<>();
                    for (KadPeer p : candidates) {
                        if (!queried.contains(p.nodeId) && toQuery.size() < concurrency) {
                            toQuery.add(p);
                            queried.add(p.nodeId);
                        }
                    }
                    if (toQuery.isEmpty()) break;

                    List<CompletableFuture<List<KadPeer>>> futures = new ArrayList<>();
                    for (KadPeer peer : toQuery) {
                        futures.add(CompletableFuture.supplyAsync(() -> {
                            try {
                                var result = protocol.sendFindNode(target, peer.nodeId).get(
                                        peerTimeout.toSeconds(), TimeUnit.SECONDS);
                                routingTable.markSeen(peer.nodeId);
                                return result.closerPeers();
                            } catch (Exception e) {
                                routingTable.remove(peer.nodeId);
                                return List.<KadPeer>of();
                            }
                        }, exec));
                    }

                    boolean progress = false;
                    CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
                    for (CompletableFuture<List<KadPeer>> f : futures) {
                        for (KadPeer closer : f.join()) {
                            if (!queried.contains(closer.nodeId)) {
                                candidates.add(closer);
                                routingTable.insert(closer.nodeId, closer.multiaddrs);
                                progress = true;
                            }
                        }
                    }
                    noProgress = progress ? 0 : noProgress + 1;
                }
            } finally {
                exec.shutdownNow();
            }
        } catch (Exception ignored) {}
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
}
