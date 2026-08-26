package com.libp2p.kademlia.lookup;

import com.libp2p.kademlia.protocol.KademliaProtocol;
import com.libp2p.kademlia.records.Record;
import com.libp2p.kademlia.routing.KadPeer;
import io.libp2p.core.PeerId;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class IterativeLookup {
    private final byte[] target;
    private final int k;
    private final int alpha;
    private final int beta;
    private final Duration peerTimeout;
    private final KademliaProtocol protocol;
    private LookupState state = LookupState.ITERATING;
    private int noProgressCount;
    private final List<PeerEntry> peers = new ArrayList<>();
    private volatile io.libp2p.core.Host host;
    private final List<com.libp2p.kademlia.records.Record> candidateRecords = new ArrayList<>();
    private final List<com.libp2p.kademlia.records.ProviderRecord> collectedProviders = new ArrayList<>();
    private final Map<PeerId, com.libp2p.kademlia.records.Record> peerRecords = new HashMap<>();
    private int recordsReceived = 0;
    private int quorum = 0;
    private final CompletableFuture<Void> cancellation = new CompletableFuture<>();

    public IterativeLookup(byte[] target, List<KadPeer> seedPeers, int k, int alpha, int beta,
                           Duration peerTimeout, KademliaProtocol protocol, int quorum) {
        this.target = target;
        this.k = k;
        this.alpha = alpha;
        this.beta = beta;
        this.peerTimeout = peerTimeout;
        this.protocol = protocol;
        this.quorum = quorum;
        for (KadPeer p : seedPeers) addHeard(p.nodeId, p.multiaddrs);
    }

    public IterativeLookup(byte[] target, List<KadPeer> seedPeers, int k, int alpha, int beta,
                           Duration peerTimeout, KademliaProtocol protocol) {
        this(target, seedPeers, k, alpha, beta, peerTimeout, protocol, 0);
    }

    public PeerId next() {
        int capacity = state == LookupState.STALLED ? Math.max(alpha, k) : alpha;
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

    public CompletableFuture<FindNodeResult> queryNext() {
        PeerId next = next();
        if (next == null) return CompletableFuture.completedFuture(new FindNodeResult(List.of(), List.of()));
        return protocol.sendFindNode(target, next)
                .thenApply(resp -> {
                    markSucceeded(next);
                    PeerEntry nextEntry = find(next);
                    updateRoutingTable(next, nextEntry != null ? nextEntry.addresses : List.of());
                    for (KadPeer p : resp.closerPeers()) {
                        if (!contains(p.nodeId)) { addHeard(p.nodeId, p.multiaddrs); }
                        updateRoutingTable(p.nodeId, p.multiaddrs);
                    }
                    checkTermination();
                    return new FindNodeResult(resp.closerPeers(), List.of(next));
                })
                .exceptionally(ex -> {
                    markFailed(next);
                    noProgressCount++;
                    if (noProgressCount >= alpha && state == LookupState.ITERATING) state = LookupState.STALLED;
                    checkTermination();
                    return new FindNodeResult(List.of(), List.of(next));
                });
    }

    public GetValueResult queryNextGetValue() {
        PeerId next = next();
        if (next == null) return new GetValueResult(null, List.of(), List.of());
        try {
            var resp = protocol.sendGetValue(target, next).get(peerTimeout.toSeconds(), TimeUnit.SECONDS);
            markSucceeded(next);
            PeerEntry nextEntry = find(next);
            updateRoutingTable(next, nextEntry != null ? nextEntry.addresses : List.of());
            for (KadPeer p : resp.closerPeers()) {
                if (!contains(p.nodeId)) addHeard(p.nodeId, p.multiaddrs);
                updateRoutingTable(p.nodeId, p.multiaddrs);
            }
            checkTermination();
            Record newRecord = resp.record().orElse(null);
            if (newRecord != null) {
                peerRecords.put(next, newRecord);
                recordsReceived++;
                candidateRecords.add(newRecord);
            }
            if (quorum > 0 && recordsReceived >= quorum) {
                state = LookupState.FINISHED;
                cancellation.complete(null);
            } else {
                checkTermination();
            }
            return new GetValueResult(newRecord, resp.closerPeers(), List.of(next));
        } catch (Exception e) {
            markFailed(next);
            noProgressCount++;
            if (noProgressCount >= alpha && state == LookupState.ITERATING) state = LookupState.STALLED;
            checkTermination();
            return new GetValueResult(null, List.of(), List.of(next));
        }
    }

    public GetProvidersResult queryNextGetProviders() {
        PeerId next = next();
        if (next == null) return new GetProvidersResult(List.of(), List.of(), List.of());
        try {
            var resp = protocol.sendGetProviders(target, next).get(peerTimeout.toSeconds(), TimeUnit.SECONDS);
            markSucceeded(next);
            PeerEntry nextEntry = find(next);
            updateRoutingTable(next, nextEntry != null ? nextEntry.addresses : List.of());
            for (KadPeer p : resp.closerPeers()) {
                if (!contains(p.nodeId)) addHeard(p.nodeId, p.multiaddrs);
                updateRoutingTable(p.nodeId, p.multiaddrs);
            }
            checkTermination();
            collectedProviders.addAll(resp.providers());
            return new GetProvidersResult(resp.providers(), resp.closerPeers(), List.of(next));
        } catch (Exception e) {
            markFailed(next);
            noProgressCount++;
            if (noProgressCount >= alpha && state == LookupState.ITERATING) state = LookupState.STALLED;
            checkTermination();
            return new GetProvidersResult(List.of(), List.of(), List.of(next));
        }
    }

    private void updateRoutingTable(PeerId peerId, List<io.libp2p.core.multiformats.Multiaddr> addrs) {
        if (host != null && addrs != null && !addrs.isEmpty()) {
            try {
                host.getAddressBook().addAddrs(peerId, 1800, addrs.toArray(io.libp2p.core.multiformats.Multiaddr[]::new));
            } catch (Exception ignored) {}
        }
    }

    public void setHost(io.libp2p.core.Host host) { this.host = host; }

    public void onResponse(PeerId peer, List<KadPeer> closerPeers) {
        markSucceeded(peer);
        PeerEntry peerEntry = find(peer);
        updateRoutingTable(peer, peerEntry != null ? peerEntry.addresses : List.of());
        boolean madeProgress = false;
        if (closerPeers != null) {
            for (KadPeer p : closerPeers) {
                if (!contains(p.nodeId)) { addHeard(p.nodeId, p.multiaddrs); madeProgress = true; }
                updateRoutingTable(p.nodeId, p.multiaddrs);
            }
        }
        noProgressCount = madeProgress ? 0 : noProgressCount + 1;
        if (noProgressCount >= alpha && state == LookupState.ITERATING) state = LookupState.STALLED;
        checkTermination();
    }

    public void onFailure(PeerId peer) {
        markFailed(peer);
        noProgressCount++;
        if (noProgressCount >= alpha && state == LookupState.ITERATING) state = LookupState.STALLED;
        checkTermination();
    }

    private boolean checkTermination() {
        List<PeerEntry> closestActive = getClosestActive(beta);
        if (closestActive.size() >= beta) {
            boolean allOk = true;
            for (PeerEntry pe : closestActive) {
                if (pe.state != PeerStateInner.SUCCEEDED) { allOk = false; break; }
            }
            if (allOk) { state = LookupState.FINISHED; cancellation.complete(null); return true; }
        }
        if (numHeard() == 0 && numWaiting() == 0) { state = LookupState.FINISHED; cancellation.complete(null); return true; }
        return false;
    }

    private void addHeard(PeerId id, List<io.libp2p.core.multiformats.Multiaddr> addrs) {
        if (contains(id)) return;
        peers.add(new PeerEntry(id, addrs));
        sortByDistance();
    }

    private boolean contains(PeerId id) { return peers.stream().anyMatch(p -> p.peerId.equals(id)); }

    private void markSucceeded(PeerId id) {
        PeerEntry e = find(id);
        if (e != null) { e.state = PeerStateInner.SUCCEEDED; e.waitSince = null; }
    }

    private void markFailed(PeerId id) {
        PeerEntry e = find(id);
        if (e != null) { e.state = PeerStateInner.FAILED; e.waitSince = null; }
    }

    private int numHeard() { return (int) peers.stream().filter(p -> p.state == PeerStateInner.NOT_CONTACTED).count(); }
    private int numWaiting() { return (int) peers.stream().filter(p -> p.state == PeerStateInner.WAITING).count(); }

    private List<PeerEntry> getClosestActive(int n) {
        List<PeerEntry> active = new ArrayList<>();
        for (PeerEntry pe : peers) {
            if (pe.state != PeerStateInner.FAILED && pe.state != PeerStateInner.UNRESPONSIVE) active.add(pe);
        }
        active.sort((a, b) -> {
            byte[] dA = com.libp2p.kademlia.XorId.xor(target, com.libp2p.kademlia.XorId.fromPeerId(a.peerId));
            byte[] dB = com.libp2p.kademlia.XorId.xor(target, com.libp2p.kademlia.XorId.fromPeerId(b.peerId));
            return com.libp2p.kademlia.XorId.compareDistance(dA, dB);
        });
        return active.size() > n ? active.subList(0, n) : active;
    }

    public List<KadPeer> getClosestPeers() {
        List<KadPeer> result = new ArrayList<>();
        for (PeerEntry pe : peers) {
            if (pe.state == PeerStateInner.SUCCEEDED)
                result.add(new KadPeer(pe.peerId, pe.addresses, KadPeer.ConnectionType.CONNECTED));
        }
        result.sort((a, b) -> {
            byte[] dA = com.libp2p.kademlia.XorId.xor(target, com.libp2p.kademlia.XorId.fromPeerId(a.nodeId));
            byte[] dB = com.libp2p.kademlia.XorId.xor(target, com.libp2p.kademlia.XorId.fromPeerId(b.nodeId));
            return com.libp2p.kademlia.XorId.compareDistance(dA, dB);
        });
        return result.size() > k ? result.subList(0, k) : result;
    }

    public List<KadPeer> getQueriedPeers() {
        return peers.stream()
                .filter(pe -> pe.state == PeerStateInner.SUCCEEDED)
                .map(pe -> new KadPeer(pe.peerId, pe.addresses, KadPeer.ConnectionType.CONNECTED))
                .toList();
    }

    private void sortByDistance() {
        peers.sort((a, b) -> {
            byte[] dA = com.libp2p.kademlia.XorId.xor(target, com.libp2p.kademlia.XorId.fromPeerId(a.peerId));
            byte[] dB = com.libp2p.kademlia.XorId.xor(target, com.libp2p.kademlia.XorId.fromPeerId(b.peerId));
            return com.libp2p.kademlia.XorId.compareDistance(dA, dB);
        });
    }

    private PeerEntry find(PeerId id) { for (PeerEntry pe : peers) if (pe.peerId.equals(id)) return pe; return null; }

    public List<PeerEntry> getAllPeerEntries() { return List.copyOf(peers); }

    public LookupState getState() { return state; }
    public boolean isFinished() { return state == LookupState.FINISHED; }
    public byte[] getTarget() { return target; }
    public com.libp2p.kademlia.records.Record getRecord() { return candidateRecords.isEmpty() ? null : candidateRecords.get(0); }
    public List<com.libp2p.kademlia.records.Record> getCandidateRecords() { return List.copyOf(candidateRecords); }
    public List<com.libp2p.kademlia.records.ProviderRecord> getProviders() { return collectedProviders; }
    public Map<PeerId, com.libp2p.kademlia.records.Record> getPeerRecords() { return Map.copyOf(peerRecords); }

    public void setBestRecord(com.libp2p.kademlia.records.Record record) {
        candidateRecords.clear();
        if (record != null) candidateRecords.add(record);
    }

    public void setPeerRecords(Map<PeerId, com.libp2p.kademlia.records.Record> records) {
        peerRecords.clear();
        peerRecords.putAll(records);
    }

    public void addCollectedProviders(List<com.libp2p.kademlia.records.ProviderRecord> providers) {
        collectedProviders.addAll(providers);
    }
    public int getAlpha() { return alpha; }
    public int getK() { return k; }
    public CompletableFuture<Void> getCancellation() { return cancellation; }
    public void addCandidateRecords(List<com.libp2p.kademlia.records.Record> records) { this.candidateRecords.addAll(records); }

    public enum PeerStateInner { NOT_CONTACTED, WAITING, UNRESPONSIVE, FAILED, SUCCEEDED }

    public record FindNodeResult(List<KadPeer> closerPeers, List<PeerId> queried) {}
    public record GetValueResult(com.libp2p.kademlia.records.Record record, List<KadPeer> closerPeers, List<PeerId> queried) {}
    public record GetProvidersResult(List<com.libp2p.kademlia.records.ProviderRecord> providers, List<KadPeer> closerPeers, List<PeerId> queried) {}

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
