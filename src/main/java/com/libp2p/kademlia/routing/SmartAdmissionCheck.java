package com.libp2p.kademlia.routing;

import com.libp2p.kademlia.protocol.KademliaProtocol;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class SmartAdmissionCheck implements AdmissionCheck {
    private final KademliaProtocol protocol;
    private final Host host;

    public SmartAdmissionCheck(KademliaProtocol protocol, Host host) {
        this.protocol = protocol;
        this.host = host;
    }

    @Override
    public CompletableFuture<Boolean> checkAdmission(PeerId peer) {
        if (host == null || protocol == null) return CompletableFuture.completedFuture(true);
        try {
            byte[] selfId = com.libp2p.kademlia.XorId.fromPeerId(host.getPeerId());
            return protocol.sendFindNode(selfId, peer)
                    .thenApply(resp -> resp.closerPeers() != null && !resp.closerPeers().isEmpty())
                    .exceptionally(ex -> false);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(false);
        }
    }
}
