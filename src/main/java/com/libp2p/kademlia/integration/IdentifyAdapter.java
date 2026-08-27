package com.libp2p.kademlia.integration;

import com.libp2p.kademlia.routing.DefaultPeerDiversityPolicy;
import com.libp2p.kademlia.routing.PeerDiversityPolicy;
import com.libp2p.kademlia.routing.RoutingTable;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class IdentifyAdapter {
    private final RoutingTable routingTable;
    private volatile Host host;
    private final String kadProtocol;
    private final Set<PeerId> kadServers = ConcurrentHashMap.newKeySet();
    private final Set<PeerId> nonKadServers = ConcurrentHashMap.newKeySet();
    private volatile PeerDiversityPolicy diversityPolicy;

    public IdentifyAdapter(RoutingTable routingTable, Host host, String kadProtocol) {
        this.routingTable = routingTable;
        this.host = host;
        this.kadProtocol = kadProtocol;
    }

    public void setHost(Host host) { this.host = host; }
    public void setDiversityPolicy(PeerDiversityPolicy policy) { this.diversityPolicy = policy; }

    public Boolean getKadServerSupport(PeerId peer) {
        if (kadServers.contains(peer)) return true;
        if (nonKadServers.contains(peer)) return false;
        return null;
    }

    public void onPeerIdentified(PeerId peer, Collection<Multiaddr> addresses, List<String> protocols) {
        if (peer.equals(host.getPeerId())) return;
        boolean supportsKad = protocols != null && protocols.stream().anyMatch(p -> p.contains("/kad/"));
        if (supportsKad) {
            kadServers.add(peer);
            nonKadServers.remove(peer);
            routingTable.insert(peer, addresses != null ? List.copyOf(addresses) : List.of());
            if (diversityPolicy instanceof DefaultPeerDiversityPolicy dp && addresses != null) {
                dp.cacheSubnet(peer, addresses);
            }
        } else {
            nonKadServers.add(peer);
            kadServers.remove(peer);
        }
    }

    public void onAddressesUpdated(PeerId peer, Collection<Multiaddr> addresses) {
        if (peer.equals(host.getPeerId())) return;
        routingTable.markSeen(peer);
        if (diversityPolicy instanceof DefaultPeerDiversityPolicy dp && addresses != null) {
            dp.cacheSubnet(peer, addresses);
        }
    }
}
