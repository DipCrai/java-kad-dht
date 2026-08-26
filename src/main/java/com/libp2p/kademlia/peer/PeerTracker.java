package com.libp2p.kademlia.peer;

import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class PeerTracker {

    private final ConcurrentHashMap<PeerId, PeerInfo> peers = new ConcurrentHashMap<>();

    public PeerInfo track(PeerId peerId, PeerState state, List<Multiaddr> addresses) {
        PeerInfo info = new PeerInfo(peerId, state, addresses);
        peers.merge(peerId, info, (existing, newValue) -> {
            existing.setState(newValue.getState());
            existing.setAddresses(newValue.getAddresses());
            existing.setLastSeen(newValue.getLastSeen());
            return existing;
        });
        return peers.get(peerId);
    }

    public PeerInfo track(PeerInfo peerInfo) {
        peers.put(peerInfo.getPeerId(), peerInfo);
        return peerInfo;
    }

    public PeerInfo remove(PeerId peerId) {
        return peers.remove(peerId);
    }

    public PeerInfo get(PeerId peerId) {
        return peers.get(peerId);
    }

    public List<PeerInfo> getByState(PeerState state) {
        List<PeerInfo> result = new ArrayList<>();
        for (PeerInfo info : peers.values()) {
            if (info.getState() == state) {
                result.add(info);
            }
        }
        return Collections.unmodifiableList(result);
    }

    public int size() {
        return peers.size();
    }

    public Collection<PeerInfo> all() {
        return Collections.unmodifiableCollection(peers.values());
    }

    public void recordSuccess(PeerId peerId) {
        PeerInfo info = peers.get(peerId);
        if (info != null) {
            info.recordSuccess();
        }
    }

    public void recordFailure(PeerId peerId) {
        PeerInfo info = peers.get(peerId);
        if (info != null) {
            info.recordFailure();
        }
    }
}
