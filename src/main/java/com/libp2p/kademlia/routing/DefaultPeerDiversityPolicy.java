package com.libp2p.kademlia.routing;

import io.libp2p.core.PeerId;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultPeerDiversityPolicy implements PeerDiversityPolicy {
    private final Map<Integer, Set<String>> bucketSubnets = new ConcurrentHashMap<>();

    @Override
    public boolean accept(PeerId peer, int bucketIndex) {
        String subnet = extractSubnet24(peer);
        if (subnet == null) return true;
        Set<String> subnets = bucketSubnets.computeIfAbsent(bucketIndex, k -> ConcurrentHashMap.newKeySet());
        return subnets.add(subnet);
    }

    private String extractSubnet24(PeerId peer) {
        byte[] raw = peer.getBytes();
        if (raw.length < 3) return null;
        return String.format("%02x:%02x:%02x", raw[0] & 0xFF, raw[1] & 0xFF, raw[2] & 0xFF);
    }
}
