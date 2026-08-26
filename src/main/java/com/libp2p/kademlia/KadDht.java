package com.libp2p.kademlia;

import com.libp2p.kademlia.config.KadConfig;
import com.libp2p.kademlia.integration.IdentifyAdapter;
import com.libp2p.kademlia.lookup.IterativeLookup;
import com.libp2p.kademlia.metrics.KadMetrics;
import com.libp2p.kademlia.peer.PeerTracker;
import com.libp2p.kademlia.protocol.KademliaProtocol;
import com.libp2p.kademlia.records.MemoryRecordStore;
import com.libp2p.kademlia.records.MemoryProviderStore;
import com.libp2p.kademlia.records.Record;
import com.libp2p.kademlia.records.RecordStore;
import com.libp2p.kademlia.records.RecordValidator;
import com.libp2p.kademlia.records.ProviderStore;
import com.libp2p.kademlia.records.ProviderRecord;
import com.libp2p.kademlia.refresh.BootstrapManager;
import com.libp2p.kademlia.refresh.RoutingTableRefresh;
import com.libp2p.kademlia.routing.KadPeer;
import com.libp2p.kademlia.routing.RoutingTable;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class KadDht {
    private final KadConfig config;
    private final KademliaProtocol protocol;
    private final RoutingTable routingTable;
    private final MemoryRecordStore recordStore;
    private final MemoryProviderStore providerStore;
    private final PeerTracker peerTracker;
    private final KadMetrics metrics;
    private final IdentifyAdapter identifyAdapter;
    private final BootstrapManager bootstrapManager;
    private final RoutingTableRefresh rtRefresh;
    private volatile Host host;
    private volatile boolean running = false;
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> gcTask;
    private ScheduledFuture<?> providerRepubTask;
    private ScheduledFuture<?> recordRepubTask;

    public KadDht(KadConfig config) {
        this.config = config;
        this.peerTracker = new PeerTracker();
        this.metrics = new KadMetrics();

        this.routingTable = new RoutingTable(null, config.getKValue(), config.getPendingTimeout());
        this.recordStore = new MemoryRecordStore(config.getMaxRecords(), config.getMaxRecordValueSize(), config.getRecordMaxAge(),
                config.getValidator() != null ? config.getValidator() : RecordValidator.NOOP);
        this.providerStore = new MemoryProviderStore(config.getMaxProvidedKeys(), config.getMaxProvidersPerKey());

        this.protocol = new KademliaProtocol(config.getProtocolName(), config.getKValue(), config.getSubstreamTimeout(), config.getProviderRecordTTL());
        this.protocol.setRoutingTable(routingTable);
        this.protocol.setRecordStore(recordStore);
        this.protocol.setProviderStore(providerStore);

        this.identifyAdapter = new IdentifyAdapter(routingTable, null, config.getProtocolName());
        this.bootstrapManager = new BootstrapManager(routingTable, null, config.getBootstrapNodes(), config.getSubstreamTimeout());
        this.rtRefresh = new RoutingTableRefresh(routingTable, null, config.getBootstrapInterval(), config.getPendingTimeout());

        this.scheduler = Executors.newScheduledThreadPool(4, r -> {
            Thread t = new Thread(r, "kad-dht");
            t.setDaemon(true);
            return t;
        });
    }

    public void setHost(Host host) {
        this.host = host;
        routingTable.setLocalPeerId(host.getPeerId());
        protocol.setHost(host);
        identifyAdapter.setHost(host);
        bootstrapManager.setHost(host);
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
        rtRefresh.start();

        gcTask = scheduler.scheduleWithFixedDelay(() -> {
            if (!running) return;
            recordStore.garbageCollect();
            providerStore.garbageCollect();
        }, 1, 1, TimeUnit.HOURS);

        providerRepubTask = scheduler.scheduleWithFixedDelay(() -> {
            if (!running) return;
            for (ProviderRecord pr : providerStore.provided()) {
                provide(pr.getKey());
            }
        }, config.getProviderPublicationInterval().toHours(), config.getProviderPublicationInterval().toHours(), TimeUnit.HOURS);

        recordRepubTask = scheduler.scheduleWithFixedDelay(() -> {
            if (!running) return;
            for (Record r : recordStore.records()) {
                putValue(r.getKey(), r.getValue());
            }
        }, config.getRecordPublicationInterval().toHours(), config.getRecordPublicationInterval().toHours(), TimeUnit.HOURS);

        return CompletableFuture.completedFuture(null);
    }

    public void close() {
        running = false;
        rtRefresh.stop();
        if (gcTask != null) gcTask.cancel(false);
        if (providerRepubTask != null) providerRepubTask.cancel(false);
        if (recordRepubTask != null) recordRepubTask.cancel(false);
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
        return protocol.sendPing(peer);
    }

    public CompletableFuture<Boolean> putValue(byte[] key, byte[] value) {
        return iterativeLookup(key)
                .thenCompose(lookup -> {
                    List<KadPeer> closest = lookup.getClosestPeers();
                    if (closest.isEmpty()) return CompletableFuture.completedFuture(true);

                    Record record = new Record(key, value, host.getPeerId().getBytes(), null);
                    record.setTimeReceived(Instant.now());
                    recordStore.put(record);
                    metrics.recordsStored.incrementAndGet();

                    List<CompletableFuture<Boolean>> futures = new ArrayList<>();
                    for (KadPeer p : closest) futures.add(protocol.sendPutValue(record, p.nodeId));
                    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).thenApply(v -> true);
                });
    }

    public CompletableFuture<Record> getValue(byte[] key) {
        return iterativeGetValueLookup(key)
                .thenCompose(result -> {
                    List<Record> collected = new ArrayList<>();
                    if (result.getRecord() != null) collected.add(result.getRecord());

                    for (Record local : recordStore.getAll(key)) {
                        if (!collected.stream().anyMatch(r -> Arrays.equals(r.getKey(), key) && Arrays.equals(r.getValue(), local.getValue()))) {
                            collected.add(local);
                        }
                    }

                    if (collected.isEmpty()) return CompletableFuture.completedFuture(null);

                    Record best = selectBestRecord(collected);

                    List<KadPeer> stalePeers = new ArrayList<>();
                    for (KadPeer p : result.getQueriedPeers()) {
                        if (!routingTable.getAllPeers().contains(p.nodeId)) continue;
                        boolean peerHadValue = result.getRecord() != null && Arrays.equals(result.getRecord().getValue(), best.getValue());
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
        Record best = records.get(0);
        for (int i = 1; i < records.size(); i++) {
            Record candidate = records.get(i);
            if (config.getValidator() != null) {
                boolean bestValid = config.getValidator().validate(best.getKey(), best.getValue());
                boolean candValid = config.getValidator().validate(candidate.getKey(), candidate.getValue());
                if (!bestValid && candValid) { best = candidate; continue; }
                if (bestValid && !candValid) continue;
                if (!bestValid && !candValid) continue;
            }
            if (candidate.getTimeReceived() != null && best.getTimeReceived() != null) {
                if (candidate.getTimeReceived().isAfter(best.getTimeReceived())) best = candidate;
            } else if (best.getTimeReceived() == null && candidate.getTimeReceived() != null) {
                best = candidate;
            }
        }
        return best;
    }

    public CompletableFuture<Boolean> provide(byte[] key) {
        return iterativeLookup(key)
                .thenCompose(lookup -> {
                    List<KadPeer> closest = lookup.getClosestPeers();
                    if (closest.isEmpty()) return CompletableFuture.completedFuture(true);

                    ProviderRecord local = new ProviderRecord(key, host.getPeerId(),
                            Instant.now().plus(config.getProviderRecordTTL()), getSelfAddresses());
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
        List<KadPeer> seed = routingTable.findClosest(target, config.getKValue());
        if (seed.isEmpty() && host != null) {
            seed = getBootstrapSeeds(target);
        }
        IterativeLookup lookup = new IterativeLookup(target, seed, config.getKValue(),
                config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol);
        if (host != null) lookup.setHost(host);
        return runIterativeLookup(lookup);
    }

    private CompletableFuture<IterativeLookup> iterativeGetValueLookup(byte[] target) {
        List<KadPeer> seed = routingTable.findClosest(target, config.getKValue());
        if (seed.isEmpty() && host != null) {
            seed = getBootstrapSeeds(target);
        }
        IterativeLookup lookup = new IterativeLookup(target, seed, config.getKValue(),
                config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol);
        if (host != null) lookup.setHost(host);
        return runGetValueLookup(lookup);
    }

    private CompletableFuture<IterativeLookup> iterativeGetProvidersLookup(byte[] target) {
        List<KadPeer> seed = routingTable.findClosest(target, config.getKValue());
        if (seed.isEmpty() && host != null) {
            seed = getBootstrapSeeds(target);
        }
        IterativeLookup lookup = new IterativeLookup(target, seed, config.getKValue(),
                config.getAlphaValue(), config.getBetaValue(), config.getSubstreamTimeout(), protocol);
        if (host != null) lookup.setHost(host);
        return runGetProvidersLookup(lookup);
    }

    private CompletableFuture<IterativeLookup> runIterativeLookup(IterativeLookup lookup) {
        return CompletableFuture.supplyAsync(() -> {
            while (!lookup.isFinished()) {
                PeerId next = lookup.next();
                if (next == null) break;
                try {
                    var result = protocol.sendFindNode(lookup.getTarget(), next).get(
                            config.getSubstreamTimeout().toSeconds(), TimeUnit.SECONDS);
                    lookup.onResponse(next, result.closerPeers());
                } catch (Exception e) {
                    lookup.onFailure(next);
                }
            }
            return lookup;
        }, scheduler);
    }

    private CompletableFuture<IterativeLookup> runGetValueLookup(IterativeLookup lookup) {
        return CompletableFuture.supplyAsync(() -> {
            while (!lookup.isFinished()) {
                PeerId next = lookup.next();
                if (next == null) break;
                IterativeLookup.GetValueResult result = lookup.queryNextGetValue();
            }
            return lookup;
        }, scheduler);
    }

    private CompletableFuture<IterativeLookup> runGetProvidersLookup(IterativeLookup lookup) {
        return CompletableFuture.supplyAsync(() -> {
            while (!lookup.isFinished()) {
                PeerId next = lookup.next();
                if (next == null) break;
                IterativeLookup.GetProvidersResult result = lookup.queryNextGetProviders();
            }
            return lookup;
        }, scheduler);
    }

    private List<KadPeer> getBootstrapSeeds(byte[] target) {
        if (host == null) return List.of();
        List<KadPeer> seeds = new ArrayList<>();
        for (PeerId peer : routingTable.getAllPeers()) {
            List<Multiaddr> addrs;
            try { addrs = new ArrayList<>(host.getAddressBook().getAddrs(peer).get(2, TimeUnit.SECONDS)); }
            catch (Exception e) { addrs = List.of(); }
            seeds.add(new KadPeer(peer, addrs, KadPeer.ConnectionType.CONNECTED));
        }
        return seeds;
    }

    public IdentifyAdapter getIdentifyAdapter() { return identifyAdapter; }

    private List<Multiaddr> getSelfAddresses() {
        try { return new ArrayList<>(host.getAddressBook().getAddrs(host.getPeerId()).get(2, TimeUnit.SECONDS)); }
        catch (Exception e) { return List.of(); }
    }
}
