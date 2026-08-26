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
    private final ConcurrentLinkedQueue<PeerId> newlyHeardPeers = new ConcurrentLinkedQueue<>();
    private volatile com.libp2p.kademlia.integration.IdentifyAdapter identifyAdapter;
    private volatile com.libp2p.kademlia.routing.RoutingTable lookupRoutingTable;
    private volatile java.util.Set<PeerId> excludedPeers;
    private long peerAddressTTLSeconds = 1800;

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
            if (pe.getState() == PeerStateInner.WAITING) {
                if (pe.getWaitSince() != null && Instant.now().isAfter(pe.getWaitSince().plus(peerTimeout))) {
                    pe.setState(PeerStateInner.UNRESPONSIVE);
                } else waiting++;
            }
        }
        if (checkTermination()) return null;
        if (waiting < capacity) {
            for (PeerEntry pe : peers) {
                if (pe.getState() == PeerStateInner.NOT_CONTACTED) {
                    pe.setState(PeerStateInner.WAITING);
                    pe.setWaitSince(Instant.now());
                    return pe.getPeerId();
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
                    updateRoutingTable(next, nextEntry != null ? nextEntry.getAddresses() : List.of());
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

    public CompletableFuture<GetValueResult> queryGetValue(PeerId peer) {
        if (peer == null) return CompletableFuture.completedFuture(new GetValueResult(null, List.of(), List.of()));
        return protocol.sendGetValue(target, peer)
                .thenApply(resp -> {
                    markSucceeded(peer);
                    PeerEntry nextEntry = find(peer);
                    updateRoutingTable(peer, nextEntry != null ? nextEntry.getAddresses() : List.of());
                    for (KadPeer p : resp.closerPeers()) {
                        if (!contains(p.nodeId)) addHeard(p.nodeId, p.multiaddrs);
                        updateRoutingTable(p.nodeId, p.multiaddrs);
                    }
                    Record newRecord = resp.record().orElse(null);
                    if (newRecord != null) {
                        peerRecords.put(peer, newRecord);
                        recordsReceived++;
                        candidateRecords.add(newRecord);
                    }
                    if (quorum > 0 && recordsReceived >= quorum) {
                        state = LookupState.FINISHED;
                        cancellation.complete(null);
                    } else {
                        checkTermination();
                    }
                    return new GetValueResult(newRecord, resp.closerPeers(), List.of(peer));
                })
                .exceptionally(ex -> {
                    markFailed(peer);
                    noProgressCount++;
                    if (noProgressCount >= alpha && state == LookupState.ITERATING) state = LookupState.STALLED;
                    checkTermination();
                    return new GetValueResult(null, List.of(), List.of(peer));
                });
    }

    public CompletableFuture<GetProvidersResult> queryGetProviders(PeerId peer) {
        if (peer == null) return CompletableFuture.completedFuture(new GetProvidersResult(List.of(), List.of(), List.of()));
        return protocol.sendGetProviders(target, peer)
                .thenApply(resp -> {
                    markSucceeded(peer);
                    PeerEntry nextEntry = find(peer);
                    updateRoutingTable(peer, nextEntry != null ? nextEntry.getAddresses() : List.of());
                    for (KadPeer p : resp.closerPeers()) {
                        if (!contains(p.nodeId)) addHeard(p.nodeId, p.multiaddrs);
                        updateRoutingTable(p.nodeId, p.multiaddrs);
                    }
                    collectedProviders.addAll(resp.providers());
                    return new GetProvidersResult(resp.providers(), resp.closerPeers(), List.of(peer));
                })
                .exceptionally(ex -> {
                    markFailed(peer);
                    noProgressCount++;
                    if (noProgressCount >= alpha && state == LookupState.ITERATING) state = LookupState.STALLED;
                    checkTermination();
                    return new GetProvidersResult(List.of(), List.of(), List.of(peer));
                });
    }

    private void updateRoutingTable(PeerId peerId, List<io.libp2p.core.multiformats.Multiaddr> addrs) {
        if (host != null && addrs != null && !addrs.isEmpty()) {
            try {
                host.getAddressBook().addAddrs(peerId, peerAddressTTLSeconds, addrs.toArray(io.libp2p.core.multiformats.Multiaddr[]::new));
            } catch (Exception ignored) {}
        }
        if (lookupRoutingTable != null) {
            Boolean kadSupport = identifyAdapter != null ? identifyAdapter.getKadServerSupport(peerId) : null;
            if (kadSupport != null && kadSupport) {
                lookupRoutingTable.insert(peerId, addrs != null ? addrs : List.of());
            }
        }
    }

    public void setHost(io.libp2p.core.Host host) { this.host = host; }

    public void onResponse(PeerId peer, List<KadPeer> closerPeers) {
        markSucceeded(peer);
        PeerEntry peerEntry = find(peer);
        updateRoutingTable(peer, peerEntry != null ? peerEntry.getAddresses() : List.of());
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
                if (pe.getState() != PeerStateInner.SUCCEEDED) { allOk = false; break; }
            }
            if (allOk) { state = LookupState.FINISHED; cancellation.complete(null); return true; }
        }
        if (numHeard() == 0 && numWaiting() == 0) { state = LookupState.FINISHED; cancellation.complete(null); return true; }
        return false;
    }

    private void addHeard(PeerId id, List<io.libp2p.core.multiformats.Multiaddr> addrs) {
        if (contains(id)) return;
        if (excludedPeers != null && excludedPeers.contains(id)) return;
        peers.add(new PeerEntry(id, addrs));
        newlyHeardPeers.add(id);
        sortByDistance();
    }

    private boolean contains(PeerId id) { return peers.stream().anyMatch(p -> p.getPeerId().equals(id)); }

    private void markSucceeded(PeerId id) {
        PeerEntry e = find(id);
        if (e != null) { e.setState(PeerStateInner.SUCCEEDED); e.setWaitSince(null); }
        if (excludedPeers != null) excludedPeers.add(id);
    }

    private void markFailed(PeerId id) {
        PeerEntry e = find(id);
        if (e != null) { e.setState(PeerStateInner.FAILED); e.setWaitSince(null); }
        if (excludedPeers != null) excludedPeers.add(id);
    }

    private int numHeard() { return (int) peers.stream().filter(p -> p.getState() == PeerStateInner.NOT_CONTACTED).count(); }
    private int numWaiting() { return (int) peers.stream().filter(p -> p.getState() == PeerStateInner.WAITING).count(); }

    private List<PeerEntry> getClosestActive(int n) {
        List<PeerEntry> active = new ArrayList<>();
        for (PeerEntry pe : peers) {
            if (pe.getState() != PeerStateInner.FAILED && pe.getState() != PeerStateInner.UNRESPONSIVE) active.add(pe);
        }
        active.sort((a, b) -> {
            byte[] dA = com.libp2p.kademlia.XorId.xor(target, com.libp2p.kademlia.XorId.fromPeerId(a.getPeerId()));
            byte[] dB = com.libp2p.kademlia.XorId.xor(target, com.libp2p.kademlia.XorId.fromPeerId(b.getPeerId()));
            return com.libp2p.kademlia.XorId.compareDistance(dA, dB);
        });
        return active.size() > n ? active.subList(0, n) : active;
    }

    public List<KadPeer> getClosestPeers() {
        List<KadPeer> result = new ArrayList<>();
        for (PeerEntry pe : peers) {
            if (pe.getState() == PeerStateInner.SUCCEEDED) {
                KadPeer.ConnectionType connType = resolveConnectionType(pe.getPeerId());
                result.add(new KadPeer(pe.getPeerId(), pe.getAddresses(), connType));
            }
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
                .filter(pe -> pe.getState() == PeerStateInner.SUCCEEDED)
                .map(pe -> new KadPeer(pe.getPeerId(), pe.getAddresses(), resolveConnectionType(pe.getPeerId())))
                .toList();
    }

    private KadPeer.ConnectionType resolveConnectionType(PeerId peerId) {
        if (host != null) {
            try {
                for (io.libp2p.core.Connection conn : host.getNetwork().getConnections()) {
                    if (conn.secureSession().getRemoteId().equals(peerId)) {
                        return KadPeer.ConnectionType.CONNECTED;
                    }
                }
            } catch (Exception ignored) {}
        }
        return KadPeer.ConnectionType.NOT_CONNECTED;
    }

    private void sortByDistance() {
        peers.sort((a, b) -> {
            byte[] dA = com.libp2p.kademlia.XorId.xor(target, com.libp2p.kademlia.XorId.fromPeerId(a.getPeerId()));
            byte[] dB = com.libp2p.kademlia.XorId.xor(target, com.libp2p.kademlia.XorId.fromPeerId(b.getPeerId()));
            return com.libp2p.kademlia.XorId.compareDistance(dA, dB);
        });
    }

    private PeerEntry find(PeerId id) { for (PeerEntry pe : peers) if (pe.getPeerId().equals(id)) return pe; return null; }

    public List<PeerEntry> getAllPeerEntries() { return List.copyOf(peers); }

    public LookupState getState() { return state; }
    public boolean isFinished() { return state == LookupState.FINISHED; }
    public byte[] getTarget() { return target; }
    public List<com.libp2p.kademlia.records.Record> getCandidateRecords() { return List.copyOf(candidateRecords); }
    public List<com.libp2p.kademlia.records.ProviderRecord> getProviders() { return collectedProviders; }
    public Map<PeerId, com.libp2p.kademlia.records.Record> getPeerRecords() { return Map.copyOf(peerRecords); }

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

    public List<PeerId> drainNewlyHeard() {
        List<PeerId> result = new ArrayList<>();
        PeerId p;
        while ((p = newlyHeardPeers.poll()) != null) result.add(p);
        return result;
    }

    public void setIdentifyAdapter(com.libp2p.kademlia.integration.IdentifyAdapter adapter) { this.identifyAdapter = adapter; }
    public void setLookupRoutingTable(com.libp2p.kademlia.routing.RoutingTable rt) { this.lookupRoutingTable = rt; }
    public void setExcludedPeers(java.util.Set<PeerId> excluded) { this.excludedPeers = excluded; }
    public void setPeerAddressTTLSeconds(long seconds) { this.peerAddressTTLSeconds = seconds; }
    public void addCandidateRecords(List<com.libp2p.kademlia.records.Record> records) { this.candidateRecords.addAll(records); }

    public enum PeerStateInner { NOT_CONTACTED, WAITING, UNRESPONSIVE, FAILED, SUCCEEDED }

    public record FindNodeResult(List<KadPeer> closerPeers, List<PeerId> queried) {}
    public record GetValueResult(com.libp2p.kademlia.records.Record record, List<KadPeer> closerPeers, List<PeerId> queried) {}
    public record GetProvidersResult(List<com.libp2p.kademlia.records.ProviderRecord> providers, List<KadPeer> closerPeers, List<PeerId> queried) {}

    public static class PeerEntry {
        private final PeerId peerId;
        private final List<io.libp2p.core.multiformats.Multiaddr> addresses;
        private PeerStateInner state;
        private Instant waitSince;

        public PeerEntry(PeerId peerId, List<io.libp2p.core.multiformats.Multiaddr> addresses) {
            this.peerId = peerId;
            this.addresses = addresses != null ? addresses : List.of();
            this.state = PeerStateInner.NOT_CONTACTED;
        }

        public PeerId getPeerId() { return peerId; }
        public List<io.libp2p.core.multiformats.Multiaddr> getAddresses() { return addresses; }
        public PeerStateInner getState() { return state; }
        public Instant getWaitSince() { return waitSince; }
        public void setState(PeerStateInner state) { this.state = state; }
        public void setWaitSince(Instant waitSince) { this.waitSince = waitSince; }
    }
}
