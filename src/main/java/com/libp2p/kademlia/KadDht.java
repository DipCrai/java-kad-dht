package com.libp2p.kademlia;

import com.libp2p.kademlia.config.KadConfig;
import com.libp2p.kademlia.integration.IdentifyAdapter;
import com.libp2p.kademlia.lookup.IterativeLookup;
import com.libp2p.kademlia.lookup.QueryScheduler;
import com.libp2p.kademlia.metrics.KadMetrics;
import com.libp2p.kademlia.peer.PeerTracker;
import com.libp2p.kademlia.protocol.KademliaProtocol;
import com.libp2p.kademlia.query.DefaultQueryFilter;
import com.libp2p.kademlia.query.QueryFilter;
import com.libp2p.kademlia.records.MemoryRecordStore;
import com.libp2p.kademlia.records.MemoryProviderStore;
import com.libp2p.kademlia.records.Record;
import com.libp2p.kademlia.records.RecordStore;
import com.libp2p.kademlia.records.RecordValidator;
import com.libp2p.kademlia.records.ProviderStore;
import com.libp2p.kademlia.records.ProviderRecord;
import com.libp2p.kademlia.records.RecordReplicationManager;
import com.libp2p.kademlia.records.ProviderReprovideManager;
import com.libp2p.kademlia.refresh.BootstrapManager;
import com.libp2p.kademlia.refresh.RoutingTableRefresh;
import com.libp2p.kademlia.routing.KadPeer;
import com.libp2p.kademlia.routing.RoutingTable;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class KadDht {
    private final KadConfig config;
    private final KademliaProtocol protocol;
    private final RoutingTable routingTable;
    private final RecordStore recordStore;
    private final ProviderStore providerStore;
    private final PeerTracker peerTracker;
    private final KadMetrics metrics;
    private final IdentifyAdapter identifyAdapter;
    private final BootstrapManager bootstrapManager;
    private final RoutingTableRefresh rtRefresh;
    private RecordReplicationManager recordReplicationManager;
    private ProviderReprovideManager providerReprovideManager;
    private volatile Host host;
    private volatile boolean running = false;
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> gcTask;
    private ScheduledFuture<?> recordRepubTask;

    public KadDht(KadConfig config) {
        this(config, null, null);
    }

    public KadDht(KadConfig config, RecordStore recordStore) {
        this(config, recordStore, null);
    }

    public KadDht(KadConfig config, RecordStore recordStore, ProviderStore providerStore) {
        this.config = config;
        this.peerTracker = new PeerTracker();
        this.metrics = new KadMetrics();

        this.routingTable = new RoutingTable(null, config.getKValue(), config.getPendingTimeout());
        this.routingTable.setDiversityPolicy(config.getPeerDiversityPolicy());
        this.recordStore = recordStore != null ? recordStore : new MemoryRecordStore(config.getMaxRecords(), config.getMaxRecordValueSize(), config.getRecordMaxAge(),
                config.getValidator() != null ? config.getValidator() : RecordValidator.NOOP);
        this.providerStore = providerStore != null ? providerStore : new MemoryProviderStore(config.getMaxProvidedKeys(), config.getMaxProvidersPerKey());

        this.protocol = new KademliaProtocol(config.getProtocolName(), config.getKValue(), config.getSubstreamTimeout(), config.getProviderRecordTTL(), config.getProviderAddrTTL(), config.getMaxInboundRequests());
        this.protocol.setRoutingTable(routingTable);
        this.protocol.setRecordStore(this.recordStore);
        this.protocol.setProviderStore(this.providerStore);
        this.protocol.setValidator(config.getValidator() != null ? config.getValidator() : RecordValidator.NOOP);

        this.identifyAdapter = new IdentifyAdapter(routingTable, null, config.getProtocolName());
        this.bootstrapManager = new BootstrapManager(routingTable, null, config.getBootstrapNodes(), config.getSubstreamTimeout(), config.getQueryTimeout());
        this.rtRefresh = new RoutingTableRefresh(routingTable, null, config.getBootstrapInterval(), config.getPendingTimeout());
        this.rtRefresh.setProtocol(protocol);

        this.scheduler = Executors.newScheduledThreadPool(4, r -> {
            Thread t = new Thread(r, "kad-dht");
            t.setDaemon(true);
            return t;
        });
    }

    public void setHost(Host host) {
        this.host = host;
        routingTable.setLocalPeerId(host.getPeerId());
        routingTable.setHost(host);
        protocol.setHost(host);
        identifyAdapter.setHost(host);
        bootstrapManager.setHost(host);
        bootstrapManager.setFindNodeFn(target -> iterativeLookup(target).thenApply(v -> null));
        rtRefresh.setHost(host);
    }

    public KademliaProtocol getProtocol() { return protocol; }
    public RoutingTable getRoutingTable() { return routingTable; }
    public RecordStore getRecordStore() { return recordStore; }
    public ProviderStore getProviderStore() { return providerStore; }
    public PeerTracker getPeerTracker() { return peerTracker; }
    public KadMetrics getMetrics() { return metrics; }
    public KadConfig getConfig() { return config; }
    public boolean isRunning() { return running; }

    public CompletableFuture<Void> start() {
        if (running) return CompletableFuture.completedFuture(null);
        if (host == null) throw new IllegalStateException("setHost() first");
        running = true;
        protocol.setServerMode(config.getMode().isServer());
        QueryScheduler.setGlobalMaxConcurrent(config.getMaxConcurrentQueries());
        rtRefresh.start();

        gcTask = scheduler.scheduleWithFixedDelay(() -> {
            if (!running) return;
            recordStore.garbageCollect();
            providerStore.garbageCollect();
        }, 1, 1, TimeUnit.HOURS);

        recordRepubTask = scheduler.scheduleWithFixedDelay(() -> {
            if (!running) return;
            for (Record r : recordStore.records()) {
                putValue(r.getKey(), r.getValue());
            }
        }, config.getRecordPublicationInterval().toHours(), config.getRecordPublicationInterval().toHours(), TimeUnit.HOURS);

        recordReplicationManager = new RecordReplicationManager(recordStore,
                key -> putValue(key, new byte[0]),
                config.getRecordReplicationInterval());
        recordReplicationManager.start();

        providerReprovideManager = new ProviderReprovideManager(providerStore,
                key -> provide(key),
                host,
                config.getProviderPublicationInterval());
        providerReprovideManager.start();

        return CompletableFuture.completedFuture(null);
    }

    public void close() {
        running = false;
        rtRefresh.stop();
        if (gcTask != null) gcTask.cancel(false);
        if (recordRepubTask != null) recordRepubTask.cancel(false);
        if (recordReplicationManager != null) recordReplicationManager.stop();
        if (providerReprovideManager != null) providerReprovideManager.stop();
        scheduler.shutdownNow();
    }

    public CompletableFuture<Void> bootstrap() {
        return bootstrapManager.bootstrap();
    }

    public CompletableFuture<List<KadPeer>> findNode(PeerId peerId) {
        byte[] target = XorId.fromPeerId(peerId);
        return iterativeLookup(target)
                .thenApply(lookup -> {
                    List<KadPeer> result = lookup.getClosestPeers();
                    for (KadPeer p : result) routingTable.insert(p.nodeId, p.multiaddrs);
                    return result;
                });
    }

    public CompletableFuture<Boolean> ping(PeerId peer) {
        return protocol.pingLiveness(peer, Duration.ofSeconds(5));
    }

    public CompletableFuture<Boolean> putValue(byte[] key, byte[] value) {
        return iterativeLookup(key)
                .thenCompose(lookup -> {
                    List<KadPeer> closest = lookup.getClosestPeers().stream()
                            .limit(config.getReplicationFactor()).toList();
                    if (closest.isEmpty()) return CompletableFuture.completedFuture(true);

                    Record record = new Record(key, value, host.getPeerId().getBytes(), null);
                    record.setTimeReceived(Instant.now());
                    recordStore.put(record);
                    metrics.recordsStored.incrementAndGet();

                    List<CompletableFuture<Boolean>> futures = new ArrayList<>();
                    for (KadPeer p : closest) futures.add(protocol.sendPutValue(record, p.nodeId));
                    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).thenApply(v -> {
                        int successes = 0;
                        for (CompletableFuture<Boolean> f : futures) {
                            try { if (f.join()) successes++; } catch (Exception ignored) {}
                        }
                        return successes >= config.getWriteQuorum();
                    });
                });
    }

    public CompletableFuture<Record> getValue(byte[] key) {
        return iterativeGetValueLookup(key)
                .thenCompose(result -> {
                    int successfulPeers = result.getQueriedPeers().size();
                    int quorum = config.getReadQuorum();

                    List<Record> collected = new ArrayList<>(result.getCandidateRecords());

                    for (Record local : recordStore.getAll(key)) {
                        if (!collected.stream().anyMatch(r -> Arrays.equals(r.getKey(), key) && Arrays.equals(r.getValue(), local.getValue()))) {
                            collected.add(local);
                        }
                    }

                    RecordValidator validator = config.getValidator();
                    if (validator != null) {
                        collected.removeIf(r -> !validator.validate(r.getKey(), r.getValue()));
                    }

                    if (collected.isEmpty()) return CompletableFuture.completedFuture(null);
                    if (successfulPeers < quorum && collected.size() == 1) {
                        return CompletableFuture.completedFuture(null);
                    }

                    Record best = selectBestRecord(collected);

                    List<KadPeer> stalePeers = new ArrayList<>();
                    Map<PeerId, Record> peerRecords = result.getPeerRecords();
                    for (KadPeer p : result.getQueriedPeers()) {
                        if (!routingTable.getAllPeers().contains(p.nodeId)) continue;
                        Record peerRec = peerRecords.get(p.nodeId);
                        boolean peerHadValue = peerRec != null && Arrays.equals(peerRec.getValue(), best.getValue());
                        if (!peerHadValue) stalePeers.add(p);
                    }

                    if (!stalePeers.isEmpty()) {
                        for (KadPeer p : stalePeers) {
                            protocol.sendPutValue(best, p.nodeId);
                        }
                    }

                    return CompletableFuture.completedFuture(best);
                });
    }

    private Record selectBestRecord(List<Record> records) {
        if (records.size() == 1) return records.get(0);
        RecordValidator validator = config.getValidator();
        if (validator == null) {
            Record best = records.get(0);
            for (int i = 1; i < records.size(); i++) {
                Record candidate = records.get(i);
                if (candidate.getTimeReceived() != null && best.getTimeReceived() != null) {
                    if (candidate.getTimeReceived().isAfter(best.getTimeReceived())) best = candidate;
                } else if (best.getTimeReceived() == null && candidate.getTimeReceived() != null) {
                    best = candidate;
                }
            }
            return best;
        }
        byte[][] values = new byte[records.size()][];
        for (int i = 0; i < records.size(); i++) values[i] = records.get(i).getValue();
        int bestIdx = validator.select(records.get(0).getKey(), values);
        return records.get(Math.max(0, Math.min(bestIdx, records.size() - 1)));
    }

    public CompletableFuture<Boolean> provide(byte[] key) {
        return iterativeLookup(key)
                .thenCompose(lookup -> {
                    List<KadPeer> closest = lookup.getClosestPeers();
                    if (closest.isEmpty()) return CompletableFuture.completedFuture(true);

                    ProviderRecord local = new ProviderRecord(key, host.getPeerId(),
                            Instant.now().plus(config.getProviderRecordTTL()), Instant.now().plus(config.getProviderAddrTTL()), getSelfAddresses());
                    providerStore.addProvider(local);
                    metrics.providersStored.incrementAndGet();

                    List<CompletableFuture<Boolean>> futures = new ArrayList<>();
                    for (KadPeer p : closest) futures.add(protocol.sendAddProvider(key, p.nodeId));
                    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).thenApply(v -> true);
                });
    }

    public CompletableFuture<List<ProviderRecord>> findProviders(byte[] key) {
        return iterativeGetProvidersLookup(key)
                .thenApply(result -> {
                    List<ProviderRecord> local = providerStore.getProviders(key);
                    List<ProviderRecord> all = new ArrayList<>(result.getProviders());
                    for (ProviderRecord pr : local) {
                        if (!all.stream().anyMatch(p -> p.getProvider().equals(pr.getProvider()))) all.add(pr);
                    }
                    return all;
                });
    }

    private CompletableFuture<IterativeLookup> iterativeLookup(byte[] target) {
        int disjointPaths = config.getDisjointPaths();
        if (disjointPaths <= 1) {
            List<KadPeer> seed = routingTable.findClosest(target, config.getKValue());
            if (seed.isEmpty() && host != null) {
                seed = getBootstrapSeeds(target);
            }
            IterativeLookup lookup = new IterativeLookup(target, seed, config.getKValue(),
                    config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol);
            if (host != null) lookup.setHost(host);
            lookup.setIdentifyAdapter(identifyAdapter);
            lookup.setLookupRoutingTable(routingTable);
            return runIterativeLookup(lookup);
        }
        List<CompletableFuture<IterativeLookup>> paths = new ArrayList<>();
        java.util.Set<PeerId> excludedPeers = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < disjointPaths; i++) {
            List<KadPeer> seed = getDisjointSeedPeers(target, i, disjointPaths);
            IterativeLookup lookup = new IterativeLookup(target, seed, config.getKValue(),
                    config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol);
            if (host != null) lookup.setHost(host);
            lookup.setIdentifyAdapter(identifyAdapter);
            lookup.setLookupRoutingTable(routingTable);
            lookup.setExcludedPeers(excludedPeers);
            paths.add(runIterativeLookup(lookup));
        }
        return CompletableFuture.allOf(paths.toArray(CompletableFuture[]::new))
                .thenApply(v -> {
                    IterativeLookup merged = null;
                    List<KadPeer> allClosest = new ArrayList<>();
                    for (CompletableFuture<IterativeLookup> f : paths) {
                        IterativeLookup l = f.join();
                        if (merged == null) merged = l;
                        allClosest.addAll(l.getClosestPeers());
                    }
                    assert merged != null;
                    Map<PeerId, KadPeer> bestByPeer = new LinkedHashMap<>();
                    for (KadPeer p : allClosest) {
                        byte[] dist = XorId.xor(target, XorId.fromPeerId(p.nodeId));
                        KadPeer existing = bestByPeer.get(p.nodeId);
                        if (existing == null) {
                            bestByPeer.put(p.nodeId, p);
                        }
                    }
                    List<KadPeer> deduped = new ArrayList<>(bestByPeer.values());
                    deduped.sort((a, b) -> {
                        byte[] dA = XorId.xor(target, XorId.fromPeerId(a.nodeId));
                        byte[] dB = XorId.xor(target, XorId.fromPeerId(b.nodeId));
                        return XorId.compareDistance(dA, dB);
                    });
                    if (deduped.size() > config.getKValue()) {
                        deduped = new ArrayList<>(deduped.subList(0, config.getKValue()));
                    }
                    return new IterativeLookup(target, deduped, config.getKValue(),
                            config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol);
                });
    }

    private CompletableFuture<IterativeLookup> iterativeGetValueLookup(byte[] target) {
        int disjointPaths = config.getDisjointPaths();
        if (disjointPaths <= 1) {
            List<KadPeer> seed = routingTable.findClosest(target, config.getKValue());
            if (seed.isEmpty() && host != null) {
                seed = getBootstrapSeeds(target);
            }
            IterativeLookup lookup = new IterativeLookup(target, seed, config.getKValue(),
                    config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol, config.getReadQuorum());
            if (host != null) lookup.setHost(host);
            lookup.setIdentifyAdapter(identifyAdapter);
            lookup.setLookupRoutingTable(routingTable);
            return runGetValueLookup(lookup);
        }
        List<CompletableFuture<IterativeLookup>> paths = new ArrayList<>();
        java.util.Set<PeerId> excludedPeers2 = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < disjointPaths; i++) {
            List<KadPeer> seed = getDisjointSeedPeers(target, i, disjointPaths);
            IterativeLookup lookup = new IterativeLookup(target, seed, config.getKValue(),
                    config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol, config.getReadQuorum());
            if (host != null) lookup.setHost(host);
            lookup.setIdentifyAdapter(identifyAdapter);
            lookup.setLookupRoutingTable(routingTable);
            lookup.setExcludedPeers(excludedPeers2);
            paths.add(runGetValueLookup(lookup));
        }
        return CompletableFuture.allOf(paths.toArray(CompletableFuture[]::new))
                .thenApply(v -> {
                    List<KadPeer> allClosest = new ArrayList<>();
                    Map<PeerId, Record> allPeerRecords = new LinkedHashMap<>();
                    List<ProviderRecord> allProviders = new ArrayList<>();
                    List<Record> allCandidateRecords = new ArrayList<>();
                    for (CompletableFuture<IterativeLookup> f : paths) {
                        IterativeLookup l = f.join();
                        allClosest.addAll(l.getClosestPeers());
                        allPeerRecords.putAll(l.getPeerRecords());
                        allProviders.addAll(l.getProviders());
                        allCandidateRecords.addAll(l.getCandidateRecords());
                    }
                    Map<PeerId, KadPeer> bestByPeer = new LinkedHashMap<>();
                    for (KadPeer p : allClosest) {
                        if (!bestByPeer.containsKey(p.nodeId)) bestByPeer.put(p.nodeId, p);
                    }
                    List<KadPeer> deduped = new ArrayList<>(bestByPeer.values());
                    deduped.sort((a, b) -> {
                        byte[] dA = XorId.xor(target, XorId.fromPeerId(a.nodeId));
                        byte[] dB = XorId.xor(target, XorId.fromPeerId(b.nodeId));
                        return XorId.compareDistance(dA, dB);
                    });
                    if (deduped.size() > config.getKValue()) {
                        deduped = new ArrayList<>(deduped.subList(0, config.getKValue()));
                    }
                    IterativeLookup merged = new IterativeLookup(target, deduped, config.getKValue(),
                            config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol, config.getReadQuorum());
                    if (host != null) merged.setHost(host);
                    merged.addCandidateRecords(allCandidateRecords);
                    merged.setPeerRecords(allPeerRecords);
                    return merged;
                });
    }

    private CompletableFuture<IterativeLookup> iterativeGetProvidersLookup(byte[] target) {
        int disjointPaths = config.getDisjointPaths();
        if (disjointPaths <= 1) {
            List<KadPeer> seed = routingTable.findClosest(target, config.getKValue());
            if (seed.isEmpty() && host != null) {
                seed = getBootstrapSeeds(target);
            }
            IterativeLookup lookup = new IterativeLookup(target, seed, config.getKValue(),
                    config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol);
            if (host != null) lookup.setHost(host);
            lookup.setIdentifyAdapter(identifyAdapter);
            lookup.setLookupRoutingTable(routingTable);
            return runGetProvidersLookup(lookup);
        }
        List<CompletableFuture<IterativeLookup>> paths = new ArrayList<>();
        java.util.Set<PeerId> excludedPeers3 = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < disjointPaths; i++) {
            List<KadPeer> seed = getDisjointSeedPeers(target, i, disjointPaths);
            IterativeLookup lookup = new IterativeLookup(target, seed, config.getKValue(),
                    config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol);
            if (host != null) lookup.setHost(host);
            lookup.setIdentifyAdapter(identifyAdapter);
            lookup.setLookupRoutingTable(routingTable);
            lookup.setExcludedPeers(excludedPeers3);
            paths.add(runGetProvidersLookup(lookup));
        }
        return CompletableFuture.allOf(paths.toArray(CompletableFuture[]::new))
                .thenApply(v -> {
                    IterativeLookup bestLookup = null;
                    List<KadPeer> allClosest = new ArrayList<>();
                    List<ProviderRecord> allProviders = new ArrayList<>();
                    for (CompletableFuture<IterativeLookup> f : paths) {
                        IterativeLookup l = f.join();
                        allClosest.addAll(l.getClosestPeers());
                        allProviders.addAll(l.getProviders());
                        if (bestLookup == null) bestLookup = l;
                    }
                    Map<PeerId, KadPeer> bestByPeer = new LinkedHashMap<>();
                    for (KadPeer p : allClosest) {
                        if (!bestByPeer.containsKey(p.nodeId)) bestByPeer.put(p.nodeId, p);
                    }
                    List<KadPeer> deduped = new ArrayList<>(bestByPeer.values());
                    deduped.sort((a, b) -> {
                        byte[] dA = XorId.xor(target, XorId.fromPeerId(a.nodeId));
                        byte[] dB = XorId.xor(target, XorId.fromPeerId(b.nodeId));
                        return XorId.compareDistance(dA, dB);
                    });
                    if (deduped.size() > config.getKValue()) {
                        deduped = new ArrayList<>(deduped.subList(0, config.getKValue()));
                    }
                    IterativeLookup merged = new IterativeLookup(target, deduped, config.getKValue(),
                            config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol);
                    if (host != null) merged.setHost(host);
                    merged.addCollectedProviders(allProviders);
                    return merged;
                });
    }

    private QueryFilter resolveQueryFilter() {
        QueryFilter filter = config.getQueryFilter();
        if (filter == null) filter = new DefaultQueryFilter(identifyAdapter);
        return filter;
    }

    private CompletableFuture<IterativeLookup> runIterativeLookup(IterativeLookup lookup) {
        java.util.concurrent.atomic.AtomicReference<QueryScheduler> qsRef = new java.util.concurrent.atomic.AtomicReference<>();
        QueryScheduler qs = new QueryScheduler(config.getAlphaValue(), lookup, next -> {
            return protocol.sendFindNode(lookup.getTarget(), next)
                    .thenAccept(result -> {
                        lookup.onResponse(next, result.closerPeers());
                        QueryScheduler q = qsRef.get();
                        if (q != null) q.submitPeers(lookup.drainNewlyHeard());
                    })
                    .exceptionally(ex -> {
                        lookup.onFailure(next);
                        return null;
                    });
        }, config.getQueryTimeout(), scheduler);
        qs.setQueryFilter(resolveQueryFilter());
        qsRef.set(qs);
        List<PeerId> initialPeers = new ArrayList<>();
        for (IterativeLookup.PeerEntry pe : lookup.getAllPeerEntries()) {
            if (pe.getState() == IterativeLookup.PeerStateInner.NOT_CONTACTED) {
                initialPeers.add(pe.getPeerId());
            }
        }
        qs.submitPeers(initialPeers);
        return qs.awaitCompletion().thenApply(v -> lookup);
    }

    private CompletableFuture<IterativeLookup> runGetValueLookup(IterativeLookup lookup) {
        QueryScheduler qs = new QueryScheduler(config.getAlphaValue(), lookup, next -> {
            return lookup.queryGetValue(next).thenApply(r -> null);
        }, config.getQueryTimeout(), scheduler);
        qs.setQueryFilter(resolveQueryFilter());
        List<PeerId> initialPeers = new ArrayList<>();
        for (IterativeLookup.PeerEntry pe : lookup.getAllPeerEntries()) {
            if (pe.getState() == IterativeLookup.PeerStateInner.NOT_CONTACTED) {
                initialPeers.add(pe.getPeerId());
            }
        }
        qs.submitPeers(initialPeers);
        return qs.awaitCompletion().thenApply(v -> lookup);
    }

    private CompletableFuture<IterativeLookup> runGetProvidersLookup(IterativeLookup lookup) {
        QueryScheduler qs = new QueryScheduler(config.getAlphaValue(), lookup, next -> {
            return lookup.queryGetProviders(next).thenApply(r -> null);
        }, config.getQueryTimeout(), scheduler);
        qs.setQueryFilter(resolveQueryFilter());
        List<PeerId> initialPeers = new ArrayList<>();
        for (IterativeLookup.PeerEntry pe : lookup.getAllPeerEntries()) {
            if (pe.getState() == IterativeLookup.PeerStateInner.NOT_CONTACTED) {
                initialPeers.add(pe.getPeerId());
            }
        }
        qs.submitPeers(initialPeers);
        return qs.awaitCompletion().thenApply(v -> lookup);
    }

    private List<KadPeer> getBootstrapSeeds(byte[] target) {
        if (host == null) return List.of();
        List<KadPeer> seeds = new ArrayList<>();
        for (PeerId peer : routingTable.getAllPeers()) {
            List<Multiaddr> addrs;
            try { addrs = new ArrayList<>(host.getAddressBook().getAddrs(peer).get(2, TimeUnit.SECONDS)); }
            catch (Exception e) { addrs = List.of(); }
            KadPeer.ConnectionType connType = routingTable.resolveConnectionType(peer);
            seeds.add(new KadPeer(peer, addrs, connType));
        }
        return seeds;
    }

    private List<KadPeer> getDisjointSeedPeers(byte[] target, int pathIndex, int totalPaths) {
        int count = config.getKValue();
        List<KadPeer> allKnown = getBootstrapSeeds(target);
        if (allKnown.isEmpty()) return List.of();
        if (totalPaths <= 1) return allKnown.size() > count ? new ArrayList<>(allKnown.subList(0, count)) : allKnown;
        java.util.Collections.shuffle(allKnown, new java.util.Random(pathIndex * 31L + java.util.Arrays.hashCode(target)));
        int start = pathIndex * count;
        int end = Math.min(start + count, allKnown.size());
        if (start >= allKnown.size()) {
            return List.of(allKnown.get(pathIndex % allKnown.size()));
        }
        return new ArrayList<>(allKnown.subList(start, end));
    }

    public IdentifyAdapter getIdentifyAdapter() { return identifyAdapter; }

    private List<Multiaddr> getSelfAddresses() {
        try { return new ArrayList<>(host.getAddressBook().getAddrs(host.getPeerId()).get(2, TimeUnit.SECONDS)); }
        catch (Exception e) { return List.of(); }
    }
}
