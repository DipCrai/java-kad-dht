package com.libp2p.kademlia;

import io.libp2p.core.PeerId;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tracks all peers involved in a single query, sorted by XOR distance to target.
 * Port of rust QueryPeerset + go qpeerset.QueryPeerset.
 */
public class QueryPeerset {
    private final byte[] target;
    private final List<PeerInfo> peers = new ArrayList<>();

    public QueryPeerset(byte[] target) {
        this.target = target;
    }

    public void addHeard(PeerId peerId, List<io.libp2p.core.multiformats.Multiaddr> addrs) {
        if (contains(peerId)) return;
        peers.add(new PeerInfo(peerId, addrs, PeerState.NOT_CONTACTED, null, null));
        sortByDistance();
    }

    public void addHeard(KadPeer peer) {
        addHeard(peer.nodeId, peer.multiaddrs);
    }

    public void markWaiting(PeerId peerId) {
        PeerInfo info = find(peerId);
        if (info != null) {
            info.state = PeerState.WAITING;
            info.waitSince = Instant.now();
        }
    }

    public void markSucceeded(PeerId peerId) {
        PeerInfo info = find(peerId);
        if (info != null) {
            info.state = PeerState.SUCCEEDED;
            info.waitSince = null;
        }
    }

    public void markFailed(PeerId peerId) {
        PeerInfo info = find(peerId);
        if (info != null) {
            info.state = PeerState.FAILED;
            info.waitSince = null;
        }
    }

    public void markUnresponsive(PeerId peerId) {
        PeerInfo info = find(peerId);
        if (info != null) {
            info.state = PeerState.UNRESPONSIVE;
            info.waitSince = null;
        }
    }

    public PeerState getState(PeerId peerId) {
        PeerInfo info = find(peerId);
        return info != null ? info.state : null;
    }

    public boolean contains(PeerId peerId) {
        return find(peerId) != null;
    }

    /**
     * Number of peers in WAITING state.
     */
    public int numWaiting() {
        int count = 0;
        for (PeerInfo p : peers) {
            if (p.state == PeerState.WAITING) count++;
        }
        return count;
    }

    /**
     * Number of peers in NOT_CONTACTED state.
     */
    public int numHeard() {
        int count = 0;
        for (PeerInfo p : peers) {
            if (p.state == PeerState.NOT_CONTACTED) count++;
        }
        return count;
    }

    /**
     * Number of peers in SUCCEEDED state.
     */
    public int numSucceeded() {
        int count = 0;
        for (PeerInfo p : peers) {
            if (p.state == PeerState.SUCCEEDED) count++;
        }
        return count;
    }

    /**
     * Check if a WAITING peer has timed out.
     */
    public boolean isTimedOut(PeerId peerId, java.time.Duration timeout) {
        PeerInfo info = find(peerId);
        if (info == null || info.state != PeerState.WAITING || info.waitSince == null) return false;
        return Instant.now().isAfter(info.waitSince.plus(timeout));
    }

    /**
     * Get the k closest peers in SUCCEEDED state.
     */
    public List<KadPeer> getClosestSucceeded(int k) {
        List<KadPeer> result = new ArrayList<>();
        for (PeerInfo p : peers) {
            if (p.state == PeerState.SUCCEEDED) {
                result.add(new KadPeer(p.peerId, p.addresses, KadPeer.ConnectionType.CONNECTED));
            }
        }
        result.sort((a, b) -> {
            byte[] dA = XorId.xor(target, XorId.fromPeerId(a.nodeId));
            byte[] dB = XorId.xor(target, XorId.fromPeerId(b.nodeId));
            return XorId.compareDistance(dA, dB);
        });
        if (result.size() > k) return result.subList(0, k);
        return result;
    }

    /**
     * Get the closest β peers among {NOT_CONTACTED, WAITING, SUCCEEDED}.
     * Used for termination check: if all β are SUCCEEDED → lookup complete.
     */
    public List<PeerInfo> getClosestActive(int beta) {
        List<PeerInfo> active = new ArrayList<>();
        for (PeerInfo p : peers) {
            if (p.state != PeerState.FAILED && p.state != PeerState.UNRESPONSIVE) {
                active.add(p);
            }
        }
        active.sort((a, b) -> {
            byte[] dA = XorId.xor(target, XorId.fromPeerId(a.peerId));
            byte[] dB = XorId.xor(target, XorId.fromPeerId(b.peerId));
            return XorId.compareDistance(dA, dB);
        });
        if (active.size() > beta) return active.subList(0, beta);
        return active;
    }

    /**
     * Get all peers that can still be contacted (NOT_CONTACTED or timed-out WAITING).
     */
    public List<PeerInfo> getContactable(java.time.Duration peerTimeout) {
        List<PeerInfo> result = new ArrayList<>();
        for (PeerInfo p : peers) {
            if (p.state == PeerState.NOT_CONTACTED) {
                result.add(p);
            } else if (p.state == PeerState.WAITING && p.waitSince != null) {
                if (Instant.now().isAfter(p.waitSince.plus(peerTimeout))) {
                    result.add(p);
                }
            }
        }
        return result;
    }

    public int size() { return peers.size(); }

    public List<PeerInfo> getAll() {
        return Collections.unmodifiableList(peers);
    }

    private void sortByDistance() {
        peers.sort((a, b) -> {
            byte[] dA = XorId.xor(target, XorId.fromPeerId(a.peerId));
            byte[] dB = XorId.xor(target, XorId.fromPeerId(b.peerId));
            return XorId.compareDistance(dA, dB);
        });
    }

    private PeerInfo find(PeerId peerId) {
        for (PeerInfo p : peers) {
            if (p.peerId.equals(peerId)) return p;
        }
        return null;
    }

    public static class PeerInfo {
        public final PeerId peerId;
        public final List<io.libp2p.core.multiformats.Multiaddr> addresses;
        public PeerState state;
        public Instant waitSince;
        public Instant lastQueryEnd;

        public PeerInfo(PeerId peerId, List<io.libp2p.core.multiformats.Multiaddr> addresses,
                        PeerState state, Instant waitSince, Instant lastQueryEnd) {
            this.peerId = peerId;
            this.addresses = addresses != null ? addresses : List.of();
            this.state = state;
            this.waitSince = waitSince;
            this.lastQueryEnd = lastQueryEnd;
        }
    }
}
