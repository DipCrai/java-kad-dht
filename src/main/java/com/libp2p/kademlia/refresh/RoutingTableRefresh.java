package com.libp2p.kademlia.refresh;

import com.libp2p.kademlia.lookup.IterativeLookup;
import com.libp2p.kademlia.lookup.QueryScheduler;
import com.libp2p.kademlia.protocol.KademliaProtocol;
import com.libp2p.kademlia.routing.KadPeer;
import com.libp2p.kademlia.routing.KBucketEntry;
import com.libp2p.kademlia.routing.RoutingTable;
import com.libp2p.kademlia.XorId;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class RoutingTableRefresh {
    private static final Duration PEER_PING_TIMEOUT = Duration.ofSeconds(10);
    private final RoutingTable routingTable;
    private volatile Host host;
    private volatile KademliaProtocol protocol;
    private final Duration refreshInterval;
    private final Duration peerTimeout;
    private final int k;
    private final int alpha;
    private final int beta;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> task;
    private final Map<Integer, Instant> lastRefreshedAt = new HashMap<>();

    public RoutingTableRefresh(RoutingTable routingTable, Host host, Duration refreshInterval,
                               Duration peerTimeout, int k, int alpha, int beta) {
        this.routingTable = routingTable;
        this.host = host;
        this.refreshInterval = refreshInterval;
        this.peerTimeout = peerTimeout;
        this.k = k;
        this.alpha = alpha;
        this.beta = beta;
    }

    public void setHost(Host host) { this.host = host; }
    public void setProtocol(KademliaProtocol protocol) { this.protocol = protocol; }

    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "rt-refresh");
            t.setDaemon(true);
            return t;
        });
        task = scheduler.scheduleWithFixedDelay(this::refresh, 0, refreshInterval.toSeconds(), TimeUnit.SECONDS);
    }

    public void stop() {
        if (task != null) task.cancel(false);
        if (scheduler != null) scheduler.shutdownNow();
    }

    private void refresh() {
        try {
            if (host == null || protocol == null) return;
            byte[] selfKey = XorId.fromPeerId(host.getPeerId());
            pingAndEvictPeers();
            queryForSelf(selfKey);
            refreshBuckets(selfKey);
        } catch (Exception ignored) {}
    }

    private void queryForSelf(byte[] selfKey) {
        runRefreshQuery(selfKey);
    }

    private void refreshBuckets(byte[] selfKey) {
        List<Integer> buckets = routingTable.getNonEmptyBucketIndices();
        Instant now = Instant.now();
        for (int i = 0; i < buckets.size(); i++) {
            int idx = buckets.get(i);
            Instant last = lastRefreshedAt.get(idx);
            if (last != null && Duration.between(last, now).compareTo(refreshInterval) < 0) {
                continue;
            }
            lastRefreshedAt.put(idx, now);
            byte[] target = (idx == 0)
                    ? flipTopBit(selfKey)
                    : XorId.generateRandomKeyForBucket(selfKey, idx);
            runRefreshQuery(target);
            // Gap logic (go-libp2p rt_refresh_manager): a CPL that becomes empty
            // only warrants refreshing the CPLs close to the gap, not all the way
            // up to the highest tracked CPL.
            if (idx > 0 && routingTable.getBucket(idx).size() == 0) {
                int lastCpl = Math.min(2 * (idx + 1), buckets.get(buckets.size() - 1));
                for (int j = i + 1; j < buckets.size() && buckets.get(j) <= lastCpl; j++) {
                    int gapIdx = buckets.get(j);
                    lastRefreshedAt.put(gapIdx, now);
                    runRefreshQuery(XorId.generateRandomKeyForBucket(selfKey, gapIdx));
                }
                return;
            }
        }
    }

    private byte[] flipTopBit(byte[] key) {
        byte[] copy = key.clone();
        copy[0] = (byte) (copy[0] ^ (byte) 0x80);
        return copy;
    }

    private void runRefreshQuery(byte[] target) {
        if (host == null || protocol == null) return;
        try {
            List<KadPeer> seed = routingTable.findClosest(target, k);
            if (seed.isEmpty()) return;

            IterativeLookup lookup = new IterativeLookup(target, target, seed, k, alpha, beta, peerTimeout, protocol);
            lookup.setHost(host);
            lookup.setLookupRoutingTable(routingTable);
            lookup.setRefreshMode(true);

            java.util.concurrent.atomic.AtomicReference<QueryScheduler> qsRef =
                    new java.util.concurrent.atomic.AtomicReference<>();
            QueryScheduler qs = new QueryScheduler(alpha, lookup, next -> {
                return protocol.sendFindNode(target, next)
                        .thenAccept(result -> {
                            lookup.onResponse(next, result.closerPeers());
                            QueryScheduler q = qsRef.get();
                            if (q != null) q.submitPeers(lookup.drainNewlyHeard());
                        })
                        .exceptionally(ex -> {
                            lookup.onFailure(next);
                            return null;
                        });
            });
            qsRef.set(qs);

            List<PeerId> initialPeers = new ArrayList<>();
            for (IterativeLookup.PeerEntry pe : lookup.getAllPeerEntries()) {
                if (pe.getState() == IterativeLookup.PeerStateInner.NOT_CONTACTED) {
                    initialPeers.add(pe.getPeerId());
                }
            }
            qs.submitPeers(initialPeers);
            qs.awaitCompletion().get(peerTimeout.toSeconds(), TimeUnit.SECONDS);
        } catch (Exception ignored) {}
    }

    private void pingAndEvictPeers() {
        Instant graceThreshold = Instant.now().minus(refreshInterval);
        List<CompletableFuture<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < routingTable.getBucketCount(); i++) {
            for (KBucketEntry entry : routingTable.getBucket(i).getEntries()) {
                Instant last = entry.getLastSeen();
                if (last != null && last.isAfter(graceThreshold)) continue;
                tasks.add(CompletableFuture.runAsync(() -> {
                    try {
                        boolean alive = protocol.pingLiveness(entry.peerId, PEER_PING_TIMEOUT)
                                .get(PEER_PING_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                        if (!alive) routingTable.remove(entry.peerId);
                    } catch (Exception e) {
                        routingTable.remove(entry.peerId);
                    }
                }));
            }
        }
        if (tasks.isEmpty()) return;
        try {
            CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0]))
                    .get(PEER_PING_TIMEOUT.toSeconds() + 5, TimeUnit.SECONDS);
        } catch (Exception ignored) {}
    }
}