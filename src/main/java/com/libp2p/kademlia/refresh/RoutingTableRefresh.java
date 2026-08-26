package com.libp2p.kademlia.refresh;

import com.libp2p.kademlia.KadDht;
import com.libp2p.kademlia.routing.RoutingTable;
import com.libp2p.kademlia.XorId;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Duration;
import java.util.List;
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
            byte[] selfKey = XorId.fromPeerId(host.getPeerId());
            List<Integer> buckets = routingTable.getNonEmptyBucketIndices();
            for (int idx : buckets) {
                if (idx == 0) continue;
                byte[] randomKey = XorId.generateRandomKeyForBucket(selfKey, idx);
                List<com.libp2p.kademlia.routing.KadPeer> closest = routingTable.findClosest(randomKey, routingTable.getK());
                for (com.libp2p.kademlia.routing.KadPeer p : closest) {
                    try { pingAndMaybeRemove(p.nodeId); } catch (Exception ignored) {}
                }
            }
        } catch (Exception ignored) {}
    }

    private void pingAndMaybeRemove(PeerId peer) {
        try {
            host.newStream(List.of("/ipfs/id/1.0.0"), peer)
                    .getController().get(5, TimeUnit.SECONDS);
            routingTable.markSeen(peer);
        } catch (Exception e) {
            routingTable.remove(peer);
        }
    }
}
