package com.libp2p.kademlia.lookup;

import com.libp2p.kademlia.routing.KadPeer;
import com.libp2p.kademlia.XorId;
import io.libp2p.core.PeerId;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class IterativeLookup {
    private LookupState state = LookupState.ITERATING;
    private int noProgressCount;
    private int resultCounter;
    private final byte[] target;
    private final int numResults;
    private final int parallelism;
    private final int beta;
    private final Duration peerTimeout;
    private final List<PeerEntry> peers = new ArrayList<>();

    public IterativeLookup(byte[] target, List<KadPeer> seedPeers, int k, int alpha, int beta, Duration peerTimeout) {
        this.target = target;
        this.numResults = k;
        this.parallelism = alpha;
        this.beta = beta;
        this.peerTimeout = peerTimeout;
        for (KadPeer p : seedPeers) addHeard(p.nodeId, p.multiaddrs);
    }

    public PeerId next() {
        int capacity = state == LookupState.STALLED ? Math.max(parallelism, numResults) : parallelism;
        int waiting = 0;
        for (PeerEntry pe : peers) {
            if (pe.state == PeerStateInner.WAITING) {
                if (pe.waitSince != null && Instant.now().isAfter(pe.waitSince.plus(peerTimeout))) {
                    pe.state = PeerStateInner.UNRESPONSIVE;
                } else waiting++;
            }
        }
        if (checkTermination()) return null;
        if (waiting < capacity) {
            for (PeerEntry pe : peers) {
                if (pe.state == PeerStateInner.NOT_CONTACTED) {
                    pe.state = PeerStateInner.WAITING;
                    pe.waitSince = Instant.now();
                    return pe.peerId;
                }
            }
        }
        return null;
    }

    public void onResponse(PeerId peer, List<KadPeer> closerPeers) {
        markSucceeded(peer);
        resultCounter++;
        boolean madeProgress = false;
        if (closerPeers != null) {
            for (KadPeer p : closerPeers) {
                if (!contains(p.nodeId)) { addHeard(p.nodeId, p.multiaddrs); madeProgress = true; }
            }
        }
        noProgressCount = (madeProgress || resultCounter < numResults) ? 0 : noProgressCount + 1;
        if (noProgressCount >= parallelism && state == LookupState.ITERATING) state = LookupState.STALLED;
        checkTermination();
    }

    public void onFailure(PeerId peer) { markFailed(peer); advanceNoProgress(); }
    public void onTimeout(PeerId peer) { markUnresponsive(peer); advanceNoProgress(); }

    private void advanceNoProgress() {
        noProgressCount++;
        if (noProgressCount >= parallelism && state == LookupState.ITERATING) state = LookupState.STALLED;
        checkTermination();
    }

    private boolean checkTermination() {
        List<PeerEntry> closestActive = getClosestActive(beta);
        if (closestActive.size() >= numResults) {
            boolean allOk = true;
            for (int i = 0; i < numResults; i++) {
                if (closestActive.get(i).state != PeerStateInner.SUCCEEDED) { allOk = false; break; }
            }
            if (allOk) { state = LookupState.FINISHED; return true; }
        }
        if (numHeard() == 0 && numWaiting() == 0) { state = LookupState.FINISHED; return true; }
        return false;
    }

    private void addHeard(PeerId id, List<io.libp2p.core.multiformats.Multiaddr> addrs) {
        if (contains(id)) return;
        peers.add(new PeerEntry(id, addrs));
        sortByDistance();
    }

    private boolean contains(PeerId id) { return peers.stream().anyMatch(p -> p.peerId.equals(id)); }

    private void markSucceeded(PeerId id) { PeerEntry e = find(id); if (e != null) { e.state = PeerStateInner.SUCCEEDED; e.waitSince = null; } }
    private void markFailed(PeerId id) { PeerEntry e = find(id); if (e != null) { e.state = PeerStateInner.FAILED; e.waitSince = null; } }
    private void markUnresponsive(PeerId id) { PeerEntry e = find(id); if (e != null) { e.state = PeerStateInner.UNRESPONSIVE; e.waitSince = null; } }

    private int numHeard() { return (int) peers.stream().filter(p -> p.state == PeerStateInner.NOT_CONTACTED).count(); }
    private int numWaiting() { return (int) peers.stream().filter(p -> p.state == PeerStateInner.WAITING).count(); }

    private List<PeerEntry> getClosestActive(int n) {
        List<PeerEntry> active = new ArrayList<>();
        for (PeerEntry pe : peers) {
            if (pe.state != PeerStateInner.FAILED && pe.state != PeerStateInner.UNRESPONSIVE) active.add(pe);
        }
        active.sort((a, b) -> {
            byte[] dA = XorId.xor(target, XorId.fromPeerId(a.peerId));
            byte[] dB = XorId.xor(target, XorId.fromPeerId(b.peerId));
            return XorId.compareDistance(dA, dB);
        });
        return active.size() > n ? active.subList(0, n) : active;
    }

    public List<KadPeer> getResult() {
        List<KadPeer> result = new ArrayList<>();
        for (PeerEntry pe : peers) {
            if (pe.state == PeerStateInner.SUCCEEDED)
                result.add(new KadPeer(pe.peerId, pe.addresses, com.libp2p.kademlia.routing.KadPeer.ConnectionType.CONNECTED));
        }
        result.sort((a, b) -> {
            byte[] dA = XorId.xor(target, XorId.fromPeerId(a.nodeId));
            byte[] dB = XorId.xor(target, XorId.fromPeerId(b.nodeId));
            return XorId.compareDistance(dA, dB);
        });
        return result.size() > numResults ? result.subList(0, numResults) : result;
    }

    private void sortByDistance() {
        peers.sort((a, b) -> {
            byte[] dA = XorId.xor(target, XorId.fromPeerId(a.peerId));
            byte[] dB = XorId.xor(target, XorId.fromPeerId(b.peerId));
            return XorId.compareDistance(dA, dB);
        });
    }

    private PeerEntry find(PeerId id) { for (PeerEntry pe : peers) if (pe.peerId.equals(id)) return pe; return null; }

    public LookupState getState() { return state; }
    public boolean isFinished() { return state == LookupState.FINISHED; }
    public int getResultCount() { return resultCounter; }
    public byte[] getTarget() { return target; }

    public enum PeerStateInner { NOT_CONTACTED, WAITING, UNRESPONSIVE, FAILED, SUCCEEDED }

    public static class PeerEntry {
        public final PeerId peerId;
        public final List<io.libp2p.core.multiformats.Multiaddr> addresses;
        public PeerStateInner state;
        public Instant waitSince;

        public PeerEntry(PeerId peerId, List<io.libp2p.core.multiformats.Multiaddr> addresses) {
            this.peerId = peerId;
            this.addresses = addresses != null ? addresses : List.of();
            this.state = PeerStateInner.NOT_CONTACTED;
        }
    }
}
