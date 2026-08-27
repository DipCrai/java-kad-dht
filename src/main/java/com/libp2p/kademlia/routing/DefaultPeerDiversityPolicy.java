package com.libp2p.kademlia.routing;

import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultPeerDiversityPolicy implements PeerDiversityPolicy {
    private final Map<Integer, Map<String, AtomicInteger>> bucketSubnets = new ConcurrentHashMap<>();
    private final Map<PeerId, String> subnetCache = new ConcurrentHashMap<>();
    private volatile Host host;

    public void setHost(Host host) {
        this.host = host;
    }

    public void cacheSubnet(PeerId peer, Collection<Multiaddr> addrs) {
        if (addrs == null || addrs.isEmpty()) return;
        String subnet = extractIpSubnet(addrs);
        if (subnet != null) {
            subnetCache.put(peer, subnet);
        }
    }

    @Override
    public boolean accept(PeerId peer, int bucketIndex) {
        String subnet = subnetCache.get(peer);
        if (subnet == null) return true;
        Map<String, AtomicInteger> subnets = bucketSubnets.computeIfAbsent(bucketIndex, k -> new ConcurrentHashMap<>());
        AtomicInteger count = subnets.computeIfAbsent(subnet, k -> new AtomicInteger(0));
        if (count.get() > 0) return false;
        count.incrementAndGet();
        return true;
    }

    @Override
    public void remove(PeerId peer, int bucketIndex) {
        String subnet = subnetCache.remove(peer);
        if (subnet == null) return;
        Map<String, AtomicInteger> subnets = bucketSubnets.get(bucketIndex);
        if (subnets == null) return;
        AtomicInteger count = subnets.get(subnet);
        if (count != null) {
            if (count.decrementAndGet() <= 0) {
                subnets.remove(subnet);
            }
        }
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
