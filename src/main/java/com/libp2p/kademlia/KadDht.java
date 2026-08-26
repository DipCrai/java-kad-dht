package com.libp2p.kademlia;

import com.libp2p.kademlia.config.KadConfig;
import com.libp2p.kademlia.integration.IdentifyAdapter;
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
        this.recordStore = new MemoryRecordStore(config.getMaxRecords(), config.getMaxRecordValueSize(), config.getRecordMaxAge(), RecordValidator.NOOP);
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
        return protocol.sendFindNode(XorId.fromPeerId(peerId), peerId)
                .thenApply(peers -> {
                    for (KadPeer p : peers) routingTable.insert(p.nodeId, p.multiaddrs);
                    return peers;
                });
    }

    public CompletableFuture<Boolean> ping(PeerId peer) {
        return protocol.sendPing(peer);
    }

    public CompletableFuture<Boolean> putValue(byte[] key, byte[] value) {
        Record record = new Record(key, value);
        record.setTimeReceived(Instant.now());
        recordStore.put(record);
        metrics.recordsStored.incrementAndGet();

        List<KadPeer> closest = routingTable.findClosest(key, config.getKValue());
        if (closest.isEmpty()) return CompletableFuture.completedFuture(true);

        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        for (KadPeer p : closest) futures.add(protocol.sendPutValue(record, p.nodeId));
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).thenApply(v -> true);
    }

    public CompletableFuture<Record> getValue(byte[] key) {
        Record local = recordStore.get(key);
        if (local != null) return CompletableFuture.completedFuture(local);

        List<KadPeer> closest = routingTable.findClosest(key, config.getKValue());
        if (closest.isEmpty()) return CompletableFuture.completedFuture(null);

        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        for (KadPeer p : closest) futures.add(protocol.sendGetValue(key, p.nodeId));
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(v -> recordStore.get(key));
    }

    public CompletableFuture<Boolean> provide(byte[] key) {
        ProviderRecord local = new ProviderRecord(key, host.getPeerId(),
                Instant.now().plus(config.getProviderRecordTTL()), getSelfAddresses());
        providerStore.addProvider(local);
        metrics.providersStored.incrementAndGet();

        List<KadPeer> closest = routingTable.findClosest(key, config.getKValue());
        if (closest.isEmpty()) return CompletableFuture.completedFuture(true);

        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        for (KadPeer p : closest) futures.add(protocol.sendAddProvider(key, p.nodeId));
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).thenApply(v -> true);
    }

    public CompletableFuture<List<ProviderRecord>> findProviders(byte[] key) {
        List<ProviderRecord> local = providerStore.getProviders(key);
        if (!local.isEmpty()) return CompletableFuture.completedFuture(local);

        List<KadPeer> closest = routingTable.findClosest(key, config.getKValue());
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (KadPeer p : closest) {
            futures.add(CompletableFuture.runAsync(() -> {
                try { protocol.sendGetProviders(key, p.nodeId).get(config.getSubstreamTimeout().toSeconds(), TimeUnit.SECONDS); } catch (Exception ignored) {}
            }));
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(v -> providerStore.getProviders(key));
    }

    public IdentifyAdapter getIdentifyAdapter() { return identifyAdapter; }

    private List<Multiaddr> getSelfAddresses() {
        try { return new ArrayList<>(host.getAddressBook().getAddrs(host.getPeerId()).get(2, TimeUnit.SECONDS)); }
        catch (Exception e) { return List.of(); }
    }
}
