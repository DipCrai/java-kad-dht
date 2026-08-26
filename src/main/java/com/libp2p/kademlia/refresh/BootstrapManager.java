package com.libp2p.kademlia.refresh;

import com.libp2p.kademlia.routing.RoutingTable;
import com.libp2p.kademlia.routing.KadPeer;
import com.libp2p.kademlia.XorId;
import io.libp2p.core.Host;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Duration;
import java.util.List;
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
        return CompletableFuture.runAsync(() -> {
            for (Multiaddr addr : bootstrapNodes) {
                try { host.getNetwork().connect(addr).get(connectTimeout.toSeconds(), TimeUnit.SECONDS); } catch (Exception ignored) {}
            }
            byte[] selfKey = XorId.fromPeerId(host.getPeerId());
            List<KadPeer> closest = routingTable.findClosest(selfKey, routingTable.getK());
            for (KadPeer p : closest) {
                try {
                    host.newStream(List.of("/ipfs/kad/1.0.0"), p.nodeId)
                            .getController().get(connectTimeout.toSeconds(), TimeUnit.SECONDS);
                } catch (Exception ignored) {}
            }
            bootstrapped = true;
        });
    }

    public boolean isBootstrapped() { return bootstrapped; }
}
