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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Main facade for the Kademlia DHT.
 *
 * <p>Provides iterative lookup, record storage, provider management,
 * bootstrap, and periodic routing table refresh.</p>
 *
 * <h3>Lifecycle</h3>
 * <pre>{@code
 * KadDht dht = new KadDht(config);
 * dht.setHost(host);   // attach to libp2p host
 * dht.start();         // register handlers, start refresh loops
 * dht.bootstrap();     // connect to bootstrap peers, self-lookup
 * // ... use DHT ...
 * dht.close();         // stop refresh, close queries, cleanup
 * }</pre>
 *
 * <h3>Thread safety</h3>
 * <p>All public methods are thread-safe. Callbacks from the network layer
 * are dispatched on internal executor threads. Callers should not block
 * on DHT futures for extended periods.</p>
 *
 * <h3>Quorum semantics</h3>
 * <ul>
 *   <li>PUT_VALUE: success when {@code writeQuorum} peers ACK</li>
 *   <li>GET_VALUE: success when {@code readQuorum} peers respond with records,
 *       or all queried peers responded</li>
 *   <li>ADD_PROVIDER / GET_PROVIDERS: best-effort (fire-and-forget for ADD,
 *       merge all responses for GET)</li>
 * </ul>
 */
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

    /**
     * Create a KadDht with default in-memory stores.
     *
     * @param config immutable configuration
     */
    public KadDht(KadConfig config) {
        this(config, null, null);
    }

    /**
     * Create a KadDht with a custom record store and default provider store.
     *
     * @param config      immutable configuration
     * @param recordStore custom record store implementation
     */
    public KadDht(KadConfig config, RecordStore recordStore) {
        this(config, recordStore, null);
    }

    /**
     * Create a KadDht with custom record and provider stores.
     *
     * @param config        immutable configuration
     * @param recordStore   custom record store, or null for default
     * @param providerStore custom provider store, or null for default
     */
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
        this.identifyAdapter.setDiversityPolicy(config.getPeerDiversityPolicy());
        this.bootstrapManager = new BootstrapManager(routingTable, null, config.getBootstrapNodes(), config.getSubstreamTimeout(), config.getQueryTimeout(), config.getBootstrapAddressTTL().toMillis());
        this.rtRefresh = new RoutingTableRefresh(routingTable, null, config.getBootstrapInterval(), config.getPendingTimeout());
        this.rtRefresh.setProtocol(protocol);

        this.scheduler = Executors.newScheduledThreadPool(4, r -> {
            Thread t = new Thread(r, "kad-dht");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Attach to a libp2p host. Must be called before {@link #start()}.
     * Registers protocol handlers and wires up identify integration.
     *
     * @param host the libp2p host
     * @throws IllegalStateException if called after start
     */
    public void setHost(Host host) {
        this.host = host;
        routingTable.setLocalPeerId(host.getPeerId());
        routingTable.setHost(host);
        protocol.setHost(host);
        host.addProtocolHandler(protocol);
        host.addProtocolHandler(new io.libp2p.protocol.Ping());
        identifyAdapter.setHost(host);
        config.getPeerDiversityPolicy().setHost(host);
        bootstrapManager.setHost(host);
        bootstrapManager.setFindNodeFn(target -> iterativeLookup(target, target).thenApply(v -> null));
        rtRefresh.setHost(host);
    }

    /**
     * Get the Kademlia protocol handler (for testing or advanced use).
     *
     * @return the protocol handler
     */
    public KademliaProtocol getProtocol() { return protocol; }

    /**
     * Get the routing table.
     *
     * @return the routing table
     */
    public RoutingTable getRoutingTable() { return routingTable; }

    /**
     * Get the record store.
     *
     * @return the record store
     */
    public RecordStore getRecordStore() { return recordStore; }

    /**
     * Get the provider store.
     *
     * @return the provider store
     */
    public ProviderStore getProviderStore() { return providerStore; }

    /**
     * Get the peer tracker (for diagnostics).
     *
     * @return the peer tracker
     */
    public PeerTracker getPeerTracker() { return peerTracker; }

    /**
     * Get the metrics counters.
     *
     * @return the metrics instance
     */
    public KadMetrics getMetrics() { return metrics; }

    /**
     * Get the immutable configuration.
     *
     * @return the config
     */
    public KadConfig getConfig() { return config; }

    /**
     * Whether the DHT is currently running.
     *
     * @return true if started and not yet closed
     */
    public boolean isRunning() { return running; }

    /**
     * Start the DHT. Must call {@link #setHost(Host)} first.
     * Registers protocol handlers, starts GC, replication, reprovide, and refresh loops.
     * Safe to call multiple times — subsequent calls are no-ops.
     *
     * @return future that completes when all background tasks are scheduled
     * @throws IllegalStateException if host is not set
     */
    public CompletableFuture<Void> start() {
        if (running) return CompletableFuture.completedFuture(null);
        if (host == null) throw new IllegalStateException("setHost() first");
        running = true;
        protocol.setServerMode(config.getMode().isServer());
        rtRefresh.start();

        gcTask = scheduler.scheduleWithFixedDelay(() -> {
            if (!running) return;
            recordStore.garbageCollect();
            providerStore.garbageCollect();
        }, 1, 1, TimeUnit.HOURS);

        recordRepubTask = scheduler.scheduleWithFixedDelay(() -> {
            if (!running) return;
            for (Record r : recordStore.records()) {
                publishRecord(r);
            }
        }, config.getRecordPublicationInterval().toHours(), config.getRecordPublicationInterval().toHours(), TimeUnit.HOURS);

        recordReplicationManager = new RecordReplicationManager(recordStore,
                record -> replicateRecord(record),
                config.getRecordReplicationInterval());
        recordReplicationManager.start();

        providerReprovideManager = new ProviderReprovideManager(providerStore,
                key -> provide(key),
                host,
                config.getProviderPublicationInterval());
        providerReprovideManager.start();

        return CompletableFuture.completedFuture(null);
    }

    /**
     * Stop the DHT and release all resources.
     * Cancels refresh loops, kills in-flight queries, shuts down executor.
     * Safe to call multiple times.
     */
    public void close() {
        running = false;
        rtRefresh.stop();
        if (gcTask != null) gcTask.cancel(false);
        if (recordRepubTask != null) recordRepubTask.cancel(false);
        if (recordReplicationManager != null) recordReplicationManager.stop();
        if (providerReprovideManager != null) providerReprovideManager.stop();
        scheduler.shutdownNow();
    }

    /**
     * Bootstrap the DHT by connecting to configured bootstrap peers.
     * Performs a self-lookup (FIND_NODE on own ID) to populate the routing table.
     * Safe to call multiple times.
     *
     * @return future that completes when bootstrap lookup finishes
     */
    public CompletableFuture<Void> bootstrap() {
        return bootstrapManager.bootstrap();
    }

    /**
     * Find the closest peers to a given peer ID.
     * Performs an iterative lookup (FIND_NODE) and inserts results into the routing table.
     *
     * @param peerId the target peer ID
     * @return future containing the K closest peers
     */
    public CompletableFuture<List<KadPeer>> findNode(PeerId peerId) {
        byte[] target = XorId.fromPeerId(peerId);
        return iterativeLookup(target, peerId.getBytes())
                .thenApply(lookup -> {
                    List<KadPeer> result = lookup.getClosestPeers();
                    for (KadPeer p : result) routingTable.insert(p.nodeId, p.multiaddrs);
                    return result;
                });
    }

    /**
     * Ping a peer to check liveness.
     *
     * @param peer the peer to ping
     * @return future completing with true if the peer responded within 5 seconds
     */
    public CompletableFuture<Boolean> ping(PeerId peer) {
        return protocol.pingLiveness(peer, Duration.ofSeconds(5));
    }

    /**
     * Store a value in the DHT under the given key.
     * Performs an iterative lookup to find closest peers, stores locally,
     * and replicates to the K closest peers. Returns true when
     * {@code writeQuorum} peers have ACKed.
     *
     * @param key   the key to store under
     * @param value the value to store
     * @return future completing with true if write quorum is reached
     */
    public CompletableFuture<Boolean> putValue(byte[] key, byte[] value) {
        return iterativeLookup(XorId.fromKey(key), key)
                .thenCompose(lookup -> {
                    List<KadPeer> closest = lookup.getClosestPeers().stream()
                            .limit(config.getReplicationFactor()).toList();
                    if (closest.isEmpty()) return CompletableFuture.completedFuture(true);

                    Record record = new Record(key, value, host.getPeerId().getBytes(), null);
                    record.setTimeReceived(Instant.now());
                    recordStore.put(record);
                    metrics.recordsStored.incrementAndGet();

                    AtomicInteger successes = new AtomicInteger(0);
                    CompletableFuture<Boolean> quorumReached = new CompletableFuture<>();
                    List<CompletableFuture<Boolean>> futures = new ArrayList<>();
                    for (KadPeer p : closest) {
                        CompletableFuture<Boolean> f = protocol.sendPutValue(record, p.nodeId)
                                .thenApply(ok -> { if (ok) successes.incrementAndGet(); return ok; });
                        futures.add(f);
                        f.whenComplete((v, ex) -> {
                            if (successes.get() >= config.getWriteQuorum() && !quorumReached.isDone()) {
                                quorumReached.complete(true);
                                for (CompletableFuture<Boolean> ff : futures) ff.cancel(true);
                            }
                        });
                    }
                    return quorumReached.exceptionally(ex -> false)
                            .thenApply(quorum -> {
                                if (!quorum) {
                                    int s = 0;
                                    for (CompletableFuture<Boolean> f : futures) {
                                        try { if (f.join()) s++; } catch (Exception e) {}
                                    }
                                    return s >= config.getWriteQuorum();
                                }
                                return true;
                            });
                });
    }

    /**
     * Retrieve a value from the DHT by key.
     * Performs an iterative lookup, collects records from peers, validates
     * them, and selects the best record (newest or validator-selected).
     * If stale peers are found, pushes the best record to them.
     *
     * @param key the key to look up
     * @return future containing the best record, or null if not found / quorum not met
     */
    public CompletableFuture<Record> getValue(byte[] key) {
        return iterativeGetValueLookup(XorId.fromKey(key), key)
                .thenCompose(result -> {
                    int successfulPeers = result.getQueriedPeers().size();
                    int quorum = config.getReadQuorum();

                    List<Record> localRecords = recordStore.getAll(key);
                    boolean localValueFound = !localRecords.isEmpty();

                    List<Record> collected = new ArrayList<>(result.getCandidateRecords());

                    for (Record local : localRecords) {
                        if (!collected.stream().anyMatch(r -> Arrays.equals(r.getKey(), key) && Arrays.equals(r.getValue(), local.getValue()))) {
                            collected.add(local);
                        }
                    }

                    RecordValidator validator = config.getValidator();
                    if (validator != null) {
                        collected.removeIf(r -> !validator.validate(r.getKey(), r.getValue()));
                    }

                    if (collected.isEmpty()) return CompletableFuture.completedFuture(null);

                    int effectiveSources = successfulPeers + (localValueFound ? 1 : 0);
                    if (collected.size() < quorum && effectiveSources < quorum) {
                        return CompletableFuture.completedFuture(null);
                    }

                    Record best = selectBestRecord(collected);

                    Map<PeerId, Record> peerRecords = result.getPeerRecords();
                    List<KadPeer> stalePeers = new ArrayList<>();
                    for (KadPeer p : result.getClosestPeers()) {
                        Record peerRec = peerRecords.get(p.nodeId);
                        boolean peerHadValue = peerRec != null && Arrays.equals(peerRec.getValue(), best.getValue());
                        if (!peerHadValue) stalePeers.add(p);
                    }

                    for (KadPeer p : stalePeers) {
                        protocol.sendPutValue(best, p.nodeId);
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

    private CompletableFuture<Boolean> replicateRecord(Record record) {
        byte[] distanceTarget = XorId.fromKey(record.getKey());
        List<KadPeer> closest = routingTable.findClosest(distanceTarget, config.getReplicationFactor());
        if (closest.isEmpty()) return CompletableFuture.completedFuture(true);
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        for (KadPeer p : closest) {
            futures.add(protocol.sendPutValue(record, p.nodeId));
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).thenApply(v -> {
            metrics.replicationSuccess.incrementAndGet();
            return true;
        });
    }

    /**
     * Publish a record to the network (re-publish + replicate).
     * Creates a fresh copy with updated expiry and replicates to K closest peers.
     *
     * @param record the record to publish
     * @return future completing with true on success
     */
    public CompletableFuture<Boolean> publishRecord(Record record) {
        Record fresh = new Record(record.getKey(), record.getValue(), record.getPublisher(),
                Instant.now().plus(config.getRecordMaxAge()));
        if (recordStore.put(fresh)) {
            metrics.recordsStored.incrementAndGet();
        }
        return replicateRecord(fresh);
    }

    /**
     * Announce that this node provides data for the given key.
     * Performs an iterative lookup, stores a provider record locally,
     * and sends ADD_PROVIDER to the K closest peers.
     *
     * @param key the content key to provide
     * @return future completing with true on success
     */
    public CompletableFuture<Boolean> provide(byte[] key) {
        return iterativeLookup(XorId.fromKey(key), key)
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

    /**
     * Find providers for a given key.
     * Performs an iterative GET_PROVIDERS lookup and merges with local provider store.
     *
     * @param key the content key
     * @return future containing all known provider records for the key
     */
    public CompletableFuture<List<ProviderRecord>> findProviders(byte[] key) {
        return iterativeGetProvidersLookup(XorId.fromKey(key), key)
                .thenApply(result -> {
                    List<ProviderRecord> local = providerStore.getProviders(key);
                    List<ProviderRecord> all = new ArrayList<>(result.getProviders());
                    for (ProviderRecord pr : local) {
                        if (!all.stream().anyMatch(p -> p.getProvider().equals(pr.getProvider()))) all.add(pr);
                    }
                    return all;
                });
    }

    private CompletableFuture<IterativeLookup> iterativeLookup(byte[] target, byte[] wireTarget) {
        int disjointPaths = config.getDisjointPaths();
        if (disjointPaths <= 1) {
            List<KadPeer> seed = routingTable.findClosest(target, config.getKValue());
            if (seed.isEmpty() && host != null) {
                seed = getBootstrapSeeds(target);
            }
            IterativeLookup lookup = new IterativeLookup(target, wireTarget, seed, config.getKValue(),
                    config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol);
            if (host != null) lookup.setHost(host);
            lookup.setPeerAddressTTLSeconds(config.getPeerAddressTTL().toSeconds());
            lookup.setIdentifyAdapter(identifyAdapter);
            lookup.setLookupRoutingTable(routingTable);
            return runIterativeLookup(lookup);
        }
        List<CompletableFuture<IterativeLookup>> paths = new ArrayList<>();
        java.util.Set<PeerId> excludedPeers = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < disjointPaths; i++) {
            List<KadPeer> seed = getDisjointSeedPeers(target, i, disjointPaths);
            IterativeLookup lookup = new IterativeLookup(target, wireTarget, seed, config.getKValue(),
                    config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol);
            if (host != null) lookup.setHost(host);
            lookup.setPeerAddressTTLSeconds(config.getPeerAddressTTL().toSeconds());
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
                    return new IterativeLookup(target, wireTarget, deduped, config.getKValue(),
                            config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol);
                });
    }

    private CompletableFuture<IterativeLookup> iterativeGetValueLookup(byte[] target, byte[] wireTarget) {
        int disjointPaths = config.getDisjointPaths();
        if (disjointPaths <= 1) {
            List<KadPeer> seed = routingTable.findClosest(target, config.getKValue());
            if (seed.isEmpty() && host != null) {
                seed = getBootstrapSeeds(target);
            }
            IterativeLookup lookup = new IterativeLookup(target, wireTarget, seed, config.getKValue(),
                    config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol, config.getReadQuorum());
            if (host != null) lookup.setHost(host);
            lookup.setPeerAddressTTLSeconds(config.getPeerAddressTTL().toSeconds());
            lookup.setIdentifyAdapter(identifyAdapter);
            lookup.setLookupRoutingTable(routingTable);
            return runGetValueLookup(lookup);
        }
        List<CompletableFuture<IterativeLookup>> paths = new ArrayList<>();
        java.util.Set<PeerId> excludedPeers2 = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < disjointPaths; i++) {
            List<KadPeer> seed = getDisjointSeedPeers(target, i, disjointPaths);
            IterativeLookup lookup = new IterativeLookup(target, wireTarget, seed, config.getKValue(),
                    config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol, config.getReadQuorum());
            if (host != null) lookup.setHost(host);
            lookup.setPeerAddressTTLSeconds(config.getPeerAddressTTL().toSeconds());
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
                    IterativeLookup merged = new IterativeLookup(target, wireTarget, deduped, config.getKValue(),
                            config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol, config.getReadQuorum());
                    if (host != null) merged.setHost(host);
                    merged.setPeerAddressTTLSeconds(config.getPeerAddressTTL().toSeconds());
                    merged.addCandidateRecords(allCandidateRecords);
                    merged.setPeerRecords(allPeerRecords);
                    return merged;
                });
    }

    private CompletableFuture<IterativeLookup> iterativeGetProvidersLookup(byte[] target, byte[] wireTarget) {
        int disjointPaths = config.getDisjointPaths();
        if (disjointPaths <= 1) {
            List<KadPeer> seed = routingTable.findClosest(target, config.getKValue());
            if (seed.isEmpty() && host != null) {
                seed = getBootstrapSeeds(target);
            }
            IterativeLookup lookup = new IterativeLookup(target, wireTarget, seed, config.getKValue(),
                    config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol);
            if (host != null) lookup.setHost(host);
            lookup.setPeerAddressTTLSeconds(config.getPeerAddressTTL().toSeconds());
            lookup.setIdentifyAdapter(identifyAdapter);
            lookup.setLookupRoutingTable(routingTable);
            return runGetProvidersLookup(lookup);
        }
        List<CompletableFuture<IterativeLookup>> paths = new ArrayList<>();
        java.util.Set<PeerId> excludedPeers3 = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < disjointPaths; i++) {
            List<KadPeer> seed = getDisjointSeedPeers(target, i, disjointPaths);
            IterativeLookup lookup = new IterativeLookup(target, wireTarget, seed, config.getKValue(),
                    config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol);
            if (host != null) lookup.setHost(host);
            lookup.setPeerAddressTTLSeconds(config.getPeerAddressTTL().toSeconds());
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
                    IterativeLookup merged = new IterativeLookup(target, wireTarget, deduped, config.getKValue(),
                            config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol);
                    if (host != null) merged.setHost(host);
                    merged.setPeerAddressTTLSeconds(config.getPeerAddressTTL().toSeconds());
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
            return protocol.sendFindNode(lookup.getWireTarget(), next)
                    .thenAccept(result -> {
                        lookup.onResponse(next, result.closerPeers());
                        QueryScheduler q = qsRef.get();
                        if (q != null) q.submitPeers(lookup.drainNewlyHeard());
                    })
                    .exceptionally(ex -> {
                        lookup.onFailure(next);
                        return null;
                    });
        }, config.getQueryTimeout(), scheduler, config.getMaxConcurrentQueries());
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
        lookup.setValidator(config.getValidator());
        QueryScheduler qs = new QueryScheduler(config.getAlphaValue(), lookup, next -> {
            return lookup.queryGetValue(next).thenApply(r -> null);
        }, config.getQueryTimeout(), scheduler, config.getMaxConcurrentQueries());
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
        }, config.getQueryTimeout(), scheduler, config.getMaxConcurrentQueries());
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
