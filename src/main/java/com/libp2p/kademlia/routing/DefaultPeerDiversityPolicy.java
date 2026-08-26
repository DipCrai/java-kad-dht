package com.libp2p.kademlia.routing;

import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class DefaultPeerDiversityPolicy implements PeerDiversityPolicy {
    private final Map<Integer, Set<String>> bucketSubnets = new ConcurrentHashMap<>();
    private volatile Host host;

    public void setHost(Host host) {
        this.host = host;
    }

    @Override
    public boolean accept(PeerId peer, int bucketIndex) {
        if (host == null) return true;
        Collection<Multiaddr> addrs;
        try {
            addrs = host.getAddressBook().getAddrs(peer).get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            return true;
        }
        String subnet = extractIpSubnet(addrs);
        if (subnet == null) return true;
        Set<String> subnets = bucketSubnets.computeIfAbsent(bucketIndex, k -> ConcurrentHashMap.newKeySet());
        return subnets.add(subnet);
    }

    private String extractIpSubnet(Collection<Multiaddr> addrs) {
        for (Multiaddr addr : addrs) {
            String s = addr.toString();
            if (s.startsWith("/ip4/")) {
                String ip = s.split("/")[2];
                String[] octets = ip.split("\\.");
                if (octets.length == 4) return octets[0] + "." + octets[1] + "." + octets[2] + ".0/24";
            } else if (s.startsWith("/ip6/")) {
                String ip = s.split("/")[2];
                String[] groups = ip.split(":");
                if (groups.length >= 3) return groups[0] + ":" + groups[1] + ":" + groups[2] + "::/48";
            }
        }
        return null;
    }
}
