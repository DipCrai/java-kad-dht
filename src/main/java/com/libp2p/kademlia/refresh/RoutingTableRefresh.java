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

public class RoutingTableRefresh {
    private final RoutingTable routingTable;
    private volatile Host host;
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
            if (host == null) return;
            byte[] selfKey = XorId.fromPeerId(host.getPeerId());
            List<Integer> buckets = routingTable.getNonEmptyBucketIndices();
            for (int idx : buckets) {
                if (idx == 0) continue;
                byte[] randomKey = XorId.generateRandomKeyForBucket(selfKey, idx);
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

            while (noProgress < 3) {
                List<PeerId> toQuery = new ArrayList<>();
                for (KadPeer p : candidates) {
                    if (!queried.contains(p.nodeId) && toQuery.size() < 3) {
                        toQuery.add(p.nodeId);
                        queried.add(p.nodeId);
                    }
                }
                if (toQuery.isEmpty()) break;

                boolean progress = false;
                for (PeerId peer : toQuery) {
                    try {
                        var resp = com.libp2p.kademlia.protocol.RpcCodec.findNode(target);
                        // simplified — full impl would use protocol.sendMessage
                        List<Multiaddr> addrs;
                        try { addrs = new ArrayList<>(host.getAddressBook().getAddrs(peer).get(5, TimeUnit.SECONDS)); }
                        catch (Exception e) { addrs = List.of(); }
                        if (!addrs.isEmpty()) {
                            routingTable.markSeen(peer);
                        }
                    } catch (Exception e) {
                        routingTable.remove(peer);
                    }
                }
                noProgress = progress ? 0 : noProgress + 1;
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
