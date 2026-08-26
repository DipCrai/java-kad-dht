package com.libp2p.kademlia.integration;

import com.libp2p.kademlia.routing.RoutingTable;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class IdentifyAdapter {
    private final RoutingTable routingTable;
    private volatile Host host;
    private final String kadProtocol;

    public IdentifyAdapter(RoutingTable routingTable, Host host, String kadProtocol) {
        this.routingTable = routingTable;
        this.host = host;
        this.kadProtocol = kadProtocol;
    }

    public void setHost(Host host) { this.host = host; }

    public void onPeerIdentified(PeerId peer, Collection<Multiaddr> addresses, List<String> protocols) {
        if (peer.equals(host.getPeerId())) return;
        boolean supportsKad = protocols != null && protocols.stream().anyMatch(p -> p.contains("/kad/"));
        if (!supportsKad) return;
        routingTable.insert(peer, addresses != null ? List.copyOf(addresses) : List.of());
    }

    public void onAddressesUpdated(PeerId peer, Collection<Multiaddr> addresses) {
        if (peer.equals(host.getPeerId())) return;
        routingTable.markSeen(peer);
    }
}
