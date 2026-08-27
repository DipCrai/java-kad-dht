package com.libp2p.kademlia.peer;

import com.libp2p.kademlia.XorId;
import com.libp2p.kademlia.routing.KadPeer;
import com.libp2p.kademlia.routing.RoutingTable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class PeerSelector {

    private final PeerTracker peerTracker;
    private final RoutingTable routingTable;

    public PeerSelector(PeerTracker peerTracker, RoutingTable routingTable) {
        this.peerTracker = peerTracker;
        this.routingTable = routingTable;
    }

    public List<KadPeer> selectForLookup(byte[] target, int count) {
        byte[] selfKey = routingTable.getLocalNodeId();
        List<KadPeer> candidates = new ArrayList<>();

        for (PeerInfo info : peerTracker.all()) {
            if (!info.getState().isUsable()) {
                continue;
            }
            byte[] peerKey = XorId.fromPeerId(info.getPeerId());
            candidates.add(new KadPeer(info.getPeerId(), info.getAddresses(), KadPeer.ConnectionType.CONNECTED));
        }

        byte[] targetKey = target.length == XorId.KEY_LENGTH ? target : padToKey(target);
        candidates.sort(Comparator.comparingInt(a -> {
            byte[] dist = XorId.distance(a.nodeId, routingTable.getLocalPeerId());
            return Integer.compareUnsigned(bytesToInt(dist), bytesToInt(XorId.distance(
                    XorId.peerIdFromRawBytes(targetKey), routingTable.getLocalPeerId())));
        }));

        return candidates.stream()
                .sorted(Comparator.comparing(a -> XorId.toHex(XorId.distance(a.nodeId, XorId.peerIdFromRawBytes(targetKey)))))
                .limit(count)
                .collect(Collectors.toList());
    }

    public List<KadPeer> selectForRefresh(int bucketIndex, int count) {
        List<KadPeer> candidates = new ArrayList<>();

        for (PeerInfo info : peerTracker.all()) {
            if (!info.getState().isUsable()) {
                continue;
            }
            byte[] peerKey = XorId.fromPeerId(info.getPeerId());
            int bucket = XorId.bucketIndex(routingTable.getLocalNodeId(), peerKey);
            if (bucket == bucketIndex) {
                candidates.add(new KadPeer(info.getPeerId(), info.getAddresses(), KadPeer.ConnectionType.CONNECTED));
            }
        }

        return candidates.stream()
                .limit(count)
                .collect(Collectors.toList());
    }

    private byte[] padToKey(byte[] data) {
        if (data.length == XorId.KEY_LENGTH) {
            return data;
        }
        byte[] result = new byte[XorId.KEY_LENGTH];
        if (data.length > XorId.KEY_LENGTH) {
            System.arraycopy(data, data.length - XorId.KEY_LENGTH, result, 0, XorId.KEY_LENGTH);
        } else {
            System.arraycopy(data, 0, result, XorId.KEY_LENGTH - data.length, data.length);
        }
        return result;
    }

    private static int bytesToInt(byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) |
                ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
    }
}
