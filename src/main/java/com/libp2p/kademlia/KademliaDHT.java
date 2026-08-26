package com.libp2p.kademlia;

import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class KademliaDHT {
    private final KademliaConfig config;
    private final KademliaProtocol protocol;
    private final QueryEngine queryEngine;
    private final MemoryRecordStore recordStore;
    private final MemoryProviderStore providerStore;
    private volatile Host host;
    private volatile KademliaMode currentMode;
    private volatile boolean running = false;
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> refreshTask;
    private ScheduledFuture<?> gcTask;

    public KademliaDHT(KademliaConfig config) {
        this.config = config;
        this.currentMode = config.mode;

        this.recordStore = new MemoryRecordStore(config.maxRecords, config.maxRecordValueSize, config.recordMaxAge, RecordValidator.NOOP);
        this.providerStore = new MemoryProviderStore(config.maxProvidedKeys, config.maxProvidersPerKey, config.providerRecordTTL);

        this.protocol = new KademliaProtocol(config);
        this.protocol.setRecordStore(recordStore);
        this.protocol.setProviderStore(providerStore);

        this.queryEngine = new QueryEngine(null, config);

        this.scheduler = Executors.newScheduledThreadPool(3, r -> {
            Thread t = new Thread(r, "kademlia-dht");
            t.setDaemon(true);
            return t;
        });
    }

    public void setHost(Host host) {
        this.host = host;
        this.protocol.setHost(host);
        this.queryEngine.setHost(host);
    }

    public KademliaProtocol getProtocol() { return protocol; }

    public CompletableFuture<Void> start() {
        if (running) return CompletableFuture.completedFuture(null);
        if (host == null) throw new IllegalStateException("setHost() first");
        running = true;

        refreshTask = scheduler.scheduleWithFixedDelay(
                this::periodicRefresh,
                config.bootstrapInterval.toSeconds(),
                config.bootstrapInterval.toSeconds(),
                TimeUnit.SECONDS);

        gcTask = scheduler.scheduleWithFixedDelay(
                this::garbageCollect,
                1, 1, TimeUnit.HOURS);

        return protocol.start();
    }

    public void stop() {
        running = false;
        if (refreshTask != null) refreshTask.cancel(false);
        if (gcTask != null) gcTask.cancel(false);
        protocol.stop();
        scheduler.shutdownNow();
    }

    public CompletableFuture<List<KadPeer>> findNode(PeerId peerId) {
        byte[] key = XorId.fromPeerId(peerId);
        return protocol.sendFindNode(key, new KadPeer(peerId, List.of(), KadPeer.ConnectionType.NOT_CONNECTED))
                .thenApply(peers -> {
                    for (KadPeer p : peers) {
                        protocol.getRoutingTable().insert(p.nodeId, p.multiaddrs);
                    }
                    return peers;
                });
    }

    public CompletableFuture<List<KadPeer>> findClosestPeers(byte[] key) {
        return queryEngine.findClosestPeers(key);
    }

    public CompletableFuture<Boolean> ping(PeerId peer) {
        return protocol.sendPing(peer);
    }

    public CompletableFuture<Boolean> putValue(byte[] key, byte[] value) {
        Record record = new Record(key, value);
        record.setTimeReceived(Instant.now());

        List<KadPeer> closest = protocol.getRoutingTable().findClosest(key, config.kValue);
        if (closest.isEmpty()) {
            recordStore.put(record);
            return CompletableFuture.completedFuture(true);
        }

        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        for (KadPeer p : closest) {
            futures.add(protocol.sendPutValue(record, p.nodeId));
        }

        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(v -> {
                    recordStore.put(record);
                    return true;
                });
    }

    public CompletableFuture<Record> getValue(byte[] key) {
        Record local = recordStore.get(key);
        if (local != null) return CompletableFuture.completedFuture(local);

        List<KadPeer> closest = protocol.getRoutingTable().findClosest(key, config.kValue);
        if (closest.isEmpty()) return CompletableFuture.completedFuture(null);

        for (KadPeer p : closest) {
            try {
                protocol.sendGetValue(key, p.nodeId).get(config.substreamTimeout.toSeconds(), TimeUnit.SECONDS);
            } catch (Exception ignored) {}
        }

        Record found = recordStore.get(key);
        return CompletableFuture.completedFuture(found);
    }

    public CompletableFuture<Boolean> provide(byte[] key) {
        ProviderRecord local = new ProviderRecord(key, host.getPeerId(),
                Instant.now().plus(config.providerRecordTTL),
                getSelfAddresses());
        providerStore.addProvider(local);

        List<KadPeer> closest = protocol.getRoutingTable().findClosest(key, config.kValue);
        if (closest.isEmpty()) return CompletableFuture.completedFuture(true);

        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        for (KadPeer p : closest) {
            futures.add(protocol.sendAddProvider(key, p.nodeId));
        }

        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(v -> true);
    }

    public CompletableFuture<List<ProviderRecord>> findProviders(byte[] key) {
        List<ProviderRecord> local = providerStore.getProviders(key);
        if (!local.isEmpty()) return CompletableFuture.completedFuture(local);

        List<KadPeer> closest = protocol.getRoutingTable().findClosest(key, config.kValue);
        List<ProviderRecord> allProviders = new ArrayList<>(local);

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (KadPeer p : closest) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    protocol.sendGetProviders(key, p.nodeId).get(config.substreamTimeout.toSeconds(), TimeUnit.SECONDS);
                } catch (Exception ignored) {}
            }));
        }

        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(v -> providerStore.getProviders(key));
    }

    public CompletableFuture<List<KadPeer>> bootstrap() {
        return protocol.runBootstrap().thenCompose(v -> queryEngine.bootstrap());
    }

    public RecordStore getRecordStore() { return recordStore; }
    public ProviderStore getProviderStore() { return providerStore; }
    public RoutingTable getRoutingTable() { return protocol.getRoutingTable(); }
    public KademliaMode getCurrentMode() { return currentMode; }
    public boolean isRunning() { return running; }

    private void periodicRefresh() {
        if (!running || host == null) return;
        try {
            List<Integer> nonEmptyBuckets = protocol.getRoutingTable().getNonEmptyBucketIndices();
            for (int bucketIdx : nonEmptyBuckets) {
                if (bucketIdx == 0) continue;
                byte[] randomKey = XorId.generateRandomKeyForBucket(
                        XorId.fromPeerId(host.getPeerId()), bucketIdx);
                protocol.sendFindNode(randomKey, new KadPeer(
                        PeerId.random(), List.of(), KadPeer.ConnectionType.NOT_CONNECTED));
            }

            byte[] selfKey = XorId.fromPeerId(host.getPeerId());
            protocol.sendFindNode(selfKey, new KadPeer(
                    PeerId.random(), List.of(), KadPeer.ConnectionType.NOT_CONNECTED));
        } catch (Exception ignored) {}
    }

    private void garbageCollect() {
        if (!running) return;
        try {
            recordStore.garbageCollect();
            providerStore.garbageCollect();
        } catch (Exception ignored) {}
    }

    private List<Multiaddr> getSelfAddresses() {
        try {
            return new ArrayList<>(host.getAddressBook().getAddrs(host.getPeerId())
                    .get(2, TimeUnit.SECONDS));
        } catch (Exception e) {
            return List.of();
        }
    }
}
