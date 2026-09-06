package com.libp2p.kademlia.refresh;

import com.libp2p.kademlia.DnsaddrResolver;
import com.libp2p.kademlia.routing.RoutingTable;
import com.libp2p.kademlia.routing.KadPeer;
import com.libp2p.kademlia.XorId;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bootstrap lifecycle: connects the configured bootstrap peers and, concurrently,
 * the HTTPS routing fallback, inserts only kad-confirmed live peers into the
 * routing table, then seeds the normal iterative lookups that grow the table.
 *
 * <p>Both sources run in parallel and bootstrap proceeds to the self lookups as
 * soon as the first kad-verified peer is inserted (first-success), so a single
 * live peer found over HTTP is used as an entry point to discover the rest via
 * the network itself.</p>
 */
public class BootstrapManager {
    private static final Duration KAD_SETTLE = Duration.ofMillis(1200);
    private static final Duration KAD_CHECK_TIMEOUT = Duration.ofMillis(6000);
    private static final Duration PROMOTE_TIMEOUT = Duration.ofMillis(8000);

    private final RoutingTable routingTable;
    private volatile Host host;
    private final List<Multiaddr> bootstrapNodes;
    private final Duration connectTimeout;
    private final Duration queryTimeout;
    private final long bootstrapAddressTTL;
    private final PeerSource httpSource;
    private final boolean httpFallbackEnabled;
    private final int httpDialLimit;
    private volatile BootstrapState state = BootstrapState.NOT_STARTED;
    private volatile CompletableFuture<Void> bootstrapFuture;
    private volatile java.util.function.Function<byte[], CompletableFuture<Void>> findNodeFn;
    private volatile java.util.function.BiFunction<PeerId, byte[], CompletableFuture<Boolean>> kadCheckFn;
    private volatile java.util.function.BiFunction<PeerId, byte[], CompletableFuture<Void>> promoteFn;
    private volatile byte[] selfKeyBytes;

    public enum BootstrapState { NOT_STARTED, RUNNING, SUCCEEDED, FAILED }

    public BootstrapManager(RoutingTable routingTable, Host host, List<Multiaddr> bootstrapNodes, Duration connectTimeout, Duration queryTimeout, long bootstrapAddressTTL) {
        this(routingTable, host, bootstrapNodes, connectTimeout, queryTimeout, bootstrapAddressTTL,
                null, false, 25);
    }

    public BootstrapManager(RoutingTable routingTable, Host host, List<Multiaddr> bootstrapNodes, Duration connectTimeout, Duration queryTimeout, long bootstrapAddressTTL,
                            PeerSource httpSource, boolean httpFallbackEnabled, int httpDialLimit) {
        this.routingTable = routingTable;
        this.host = host;
        this.bootstrapNodes = bootstrapNodes;
        this.connectTimeout = connectTimeout;
        this.queryTimeout = queryTimeout;
        this.bootstrapAddressTTL = bootstrapAddressTTL;
        this.httpSource = httpSource;
        this.httpFallbackEnabled = httpFallbackEnabled;
        this.httpDialLimit = Math.max(1, httpDialLimit);
    }

    public void setHost(Host host) { this.host = host; }
    public void setFindNodeFn(java.util.function.Function<byte[], CompletableFuture<Void>> fn) { this.findNodeFn = fn; }

    /**
     * Optional kad-liveness probe used before inserting a dialed peer into the
     * routing table: only peers that actually answer a kad request are kept, so
     * silent/blackholed TCP peers cannot clog the table and stall lookups.
     */
    public void setKadCheckFn(java.util.function.BiFunction<PeerId, byte[], CompletableFuture<Boolean>> fn) { this.kadCheckFn = fn; }

    /**
     * Optional closer-peer promotion: given a kad-verified peer and the local
     * peer key, asks that peer for the nearest peers and inserts them into the
     * routing table, giving the bootstrap a warm set of peers to start from.
     */
    public void setPromoteFn(java.util.function.BiFunction<PeerId, byte[], CompletableFuture<Void>> fn) { this.promoteFn = fn; }

    public CompletableFuture<Void> bootstrap() {
        if (state == BootstrapState.RUNNING && bootstrapFuture != null) {
            return bootstrapFuture;
        }
        if (state == BootstrapState.SUCCEEDED) {
            return CompletableFuture.completedFuture(null);
        }
        state = BootstrapState.RUNNING;
        selfKeyBytes = XorId.fromPeerId(host != null ? host.getPeerId() : routingTable.getLocalPeerId());

        List<PeerId> verifiedPeers = new java.util.concurrent.CopyOnWriteArrayList<>();
        AtomicInteger officialsVerified = new AtomicInteger();
        CompletableFuture<Void> entryGate = new CompletableFuture<>();
        java.util.function.Consumer<PeerId> onVerified = peer -> {
            verifiedPeers.add(peer);
            entryGate.complete(null);
        };
        java.util.function.Consumer<PeerId> onOfficialVerified = peer -> {
            officialsVerified.incrementAndGet();
            onVerified.accept(peer);
        };

        // Both address sources run in parallel; bootstrap proceeds as soon as the
        // first kad-verified peer lands (or both sources are exhausted).
        CompletableFuture<Void> officialPhase = connectBootstrapNodes(onOfficialVerified).exceptionally(ex -> null);
        CompletableFuture<Void> httpPhase = httpFallback(onVerified).exceptionally(ex -> null);
        officialPhase.thenRun(() -> httpPhase.thenRun(() -> entryGate.complete(null)));

        CompletableFuture<Void> chain = entryGate.thenCompose(v -> growTable(verifiedPeers, officialsVerified.get() > 0));
        if (queryTimeout != null) {
            chain = chain.orTimeout(queryTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
        bootstrapFuture = chain.whenComplete((v, ex) -> {
            state = ex == null ? BootstrapState.SUCCEEDED : BootstrapState.FAILED;
            bootstrapFuture = null;
        });
        return bootstrapFuture;
    }

    /**
     * Grows the routing table once the entry point is found. On a healthy network
     * (a configured bootstrap peer kad-answered) the classic self-lookup plus
     * bucket refresh runs, which is fast when peers respond. Otherwise the fast
     * path applies: the first verified live peer is asked for its nearest peers
     * to the local key and those addresses are inserted directly, keeping the
     * bootstrap bounded instead of stalling on silent peers.
     */
    private CompletableFuture<Void> growTable(List<PeerId> verifiedPeers, boolean healthyOfficials) {
        if (host == null) return CompletableFuture.completedFuture(null);
        if (healthyOfficials) {
            return selfLookup().thenCompose(v -> refreshBuckets());
        }
        if (verifiedPeers.isEmpty() || promoteFn == null) {
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<Void>> futs = new ArrayList<>();
        int n = Math.min(2, verifiedPeers.size());
        for (int i = 0; i < n; i++) {
            try {
                futs.add(promoteFn.apply(verifiedPeers.get(i), selfKeyBytes)
                        .orTimeout(PROMOTE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                        .exceptionally(ex -> null));
            } catch (Exception ignored) {}
        }
        return CompletableFuture.allOf(futs.toArray(CompletableFuture[]::new));
    }

    private CompletableFuture<Void> connectBootstrapNodes(java.util.function.Consumer<PeerId> onVerified) {
        if (bootstrapNodes.isEmpty() || host == null) return CompletableFuture.completedFuture(null);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Multiaddr addr : expandedBootstrapAddrs()) {
            try {
                String addrStr = addr.toString();
                String[] parts = addrStr.split("/p2p/");
                if (parts.length > 1) {
                    PeerId peerId = PeerId.fromBase58(parts[1]);
                    host.getAddressBook().addAddrs(peerId, bootstrapAddressTTL, addr);
                    futures.add(host.getNetwork().connect(peerId, addr)
                            .orTimeout(connectTimeout.toMillis(), TimeUnit.MILLISECONDS)
                            .thenCompose(conn -> verifyAndInsert(peerId, addr, null, onVerified))
                            .exceptionally(ex -> null));
                }
            } catch (Exception ignored) {}
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    /**
     * HTTPS routing fallback, run in parallel with the real bootstrap peers. It
     * dials the candidates returned by the public routing servers and hands the
     * kad-verified live ones to the routing table; that single live peer then
     * acts as the entry point through which the self lookups discover the rest
     * of the network. Falls back to fully covering the bootstrap when the
     * configured real peers are dead, missing or silent.
     */
    private CompletableFuture<Void> httpFallback(java.util.function.Consumer<PeerId> onVerified) {
        if (!httpFallbackEnabled || httpSource == null || host == null) {
            return CompletableFuture.completedFuture(null);
        }
        Set<PeerId> known = routingTable.getAllPeers();
        Set<PeerId> succeeded = new HashSet<>();
        return httpSource.fetch(host.getPeerId(), httpSource.getLookupKeys())
                .thenCompose(addrs -> {
                    List<CompletableFuture<Void>> futures = new ArrayList<>();
                    int dialed = 0;
                    for (Multiaddr addr : addrs) {
                        if (dialed >= httpDialLimit) break;
                        try {
                            String addrStr = addr.toString();
                            String[] parts = addrStr.split("/p2p/");
                            if (parts.length < 2) continue;
                            PeerId peerId = PeerId.fromBase58(parts[1]);
                            if (known.contains(peerId) || succeeded.contains(peerId)) continue;
                            dialed++;
                            host.getAddressBook().addAddrs(peerId, bootstrapAddressTTL, addr);
                            futures.add(host.getNetwork().connect(peerId, addr)
                                    .orTimeout(connectTimeout.toMillis(), TimeUnit.MILLISECONDS)
                                    .thenCompose(conn -> verifyAndInsert(peerId, addr, succeeded, onVerified))
                                    .exceptionally(ex -> null));
                        } catch (Exception ignored) {}
                    }
                    return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
                })
                .exceptionally(ex -> null);
    }

    /**
     * Settle-then-verify a freshly dialed peer and insert it into the routing
     * table on success. The kad probe runs after a short settle (fresh dials are
     * often not yet ready to serve kad) and the batch-return path is hard-capped
     * so silent/blackholed peers fail fast instead of stalling the bootstrap.
     * Insertion follows the <em>real</em> kad future: peers that answer after
     * the cap still land in the routing table instead of being dropped.
     */
    private CompletableFuture<Void> verifyAndInsert(PeerId peerId, Multiaddr addr, Set<PeerId> succeeded, java.util.function.Consumer<PeerId> onVerified) {
        if (kadCheckFn == null) {
            if (succeeded == null || succeeded.add(peerId)) {
                routingTable.insert(peerId, List.of(addr));
                onVerified.accept(peerId);
            }
            return CompletableFuture.completedFuture(null);
        }
        try {
            return CompletableFuture.supplyAsync(() -> null, CompletableFuture.delayedExecutor(KAD_SETTLE.toMillis(), TimeUnit.MILLISECONDS))
                    .thenCompose(x -> {
                        CompletableFuture<Boolean> kad;
                        try {
                            kad = kadCheckFn.apply(peerId, selfKeyBytes);
                        } catch (Exception e) {
                            return CompletableFuture.completedFuture(null);
                        }
                        kad.thenAccept(ok -> {
                            if (ok && (succeeded == null || succeeded.add(peerId))) {
                                routingTable.insert(peerId, List.of(addr));
                                onVerified.accept(peerId);
                            }
                        }).exceptionally(ex -> null);
                        return kad.handle((ok, ex) -> (Void) null)
                                .orTimeout(KAD_CHECK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
                                .exceptionally(ex -> null);
                    });
        } catch (Exception e) {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Resolves DNS-based bootstrap multiaddrs ({@code /dnsaddr/...}, {@code /dns4/...},
     * {@code /dns6/...}, {@code /dns/...}) into concrete {@code /ip4|/ip6} addresses on
     * every bootstrap round, staying in sync with DNS changes the same way go-libp2p does.
     */
    private List<Multiaddr> expandedBootstrapAddrs() {
        List<Multiaddr> out = new ArrayList<>();
        for (Multiaddr addr : bootstrapNodes) {
            String as = addr.toString();
            if (DnsaddrResolver.isResolvable(as)) {
                out.addAll(DnsaddrResolver.resolve(as));
            } else {
                out.add(addr);
            }
        }
        return out;
    }

    private CompletableFuture<Void> selfLookup() {
        if (host == null) return CompletableFuture.completedFuture(null);
        byte[] selfKey = XorId.fromPeerId(host.getPeerId());
        return iterativeFindNode(selfKey);
    }

    private CompletableFuture<Void> refreshBuckets() {
        if (host == null) return CompletableFuture.completedFuture(null);
        byte[] selfKey = XorId.fromPeerId(host.getPeerId());
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 256; i++) {
            if (routingTable.getBucket(i).size() > 0) {
                byte[] randomKey = XorId.generateRandomKeyForBucket(selfKey, i);
                futures.add(iterativeFindNode(randomKey));
            }
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    }

    private CompletableFuture<Void> iterativeFindNode(byte[] target) {
        if (findNodeFn == null) throw new IllegalStateException("findNodeFn not set");
        return findNodeFn.apply(target);
    }
}