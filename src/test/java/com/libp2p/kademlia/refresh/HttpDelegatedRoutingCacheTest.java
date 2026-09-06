package com.libp2p.kademlia.refresh;

import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Single-flight and caching behaviour of {@link HttpDelegatedRouting}'s
 * responsable resolution (#8/#9): concurrent callers for the same key must
 * share one in-flight router query, failed fetches must never be pinned, and
 * returned lists must not alias the cache.
 */
class HttpDelegatedRoutingCacheTest {

    private static final byte[] KEY = {9, 9, 9, 9};
    private static final Multiaddr ADDR = Multiaddr.fromString(
            "/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWHQnFuULtcGg9axasidmqVg8HHRw2XzKHPP8LjJpiwwHn");
    private static final Multiaddr SRC_EXTRA = Multiaddr.fromString(
            "/ip4/127.0.0.1/tcp/4002/p2p/12D3KooWKnDdG3iXw9eTFijk3EWSunZcFi54Zka4wmtqtt6rPxc8");

    @Test
    void concurrentCallersForSameKeyShareOneInFlightFetch() throws Exception {
        AtomicInteger fetches = new AtomicInteger();
        HttpDelegatedRouting routing = routing(fetch(() -> {
            fetches.incrementAndGet();
            CompletableFuture<List<Multiaddr>> f = new CompletableFuture<>();
            Thread t = new Thread(() -> {
                try { Thread.sleep(300); } catch (InterruptedException ignored) {}
                f.complete(List.of(ADDR));
            });
            t.setDaemon(true);
            t.start();
            return f;
        }));

        CompletableFuture<List<Multiaddr>> first = routing.responsables(KEY);
        Thread.sleep(50); // let the first fetch register before the stampede
        CompletableFuture<List<Multiaddr>>[] others = new CompletableFuture[9];
        for (int i = 0; i < 9; i++) others[i] = routing.responsables(KEY);

        assertFalse(first.get(5, TimeUnit.SECONDS).isEmpty());
        for (CompletableFuture<List<Multiaddr>> f : others) assertFalse(f.get(5, TimeUnit.SECONDS).isEmpty());
        assertEquals(1, fetches.get(), "one in-flight fetch must serve every concurrent caller (#8)");
    }

    @Test
    void cachedResultServedWithoutRefetchWithinTtl() throws Exception {
        AtomicInteger fetches = new AtomicInteger();
        HttpDelegatedRouting routing = routing(fetch(() -> {
            fetches.incrementAndGet();
            return CompletableFuture.completedFuture(List.of(ADDR));
        }));

        assertFalse(routing.responsables(KEY).get(1, TimeUnit.SECONDS).isEmpty());
        assertFalse(routing.responsables(KEY).get(1, TimeUnit.SECONDS).isEmpty());
        assertEquals(1, fetches.get(), "second call within TTL must come from the cache");
    }

    @Test
    void failedFetchIsNeverPinnedAndNextCallRefetches() throws Exception {
        AtomicInteger fetches = new AtomicInteger();
        HttpDelegatedRouting routing = routing(fetch(() -> {
            if (fetches.incrementAndGet() == 1) {
                CompletableFuture<List<Multiaddr>> f = new CompletableFuture<>();
                f.completeExceptionally(new IllegalStateException("router down"));
                return f;
            }
            return CompletableFuture.completedFuture(List.of(ADDR));
        }));

        assertTrue(routing.responsables(KEY).get(1, TimeUnit.SECONDS).isEmpty());
        assertEquals(1, fetches.get());
        assertFalse(routing.responsables(KEY).get(1, TimeUnit.SECONDS).isEmpty());
        assertEquals(2, fetches.get(), "a failed fetch must not poison the cache for later calls");
    }

    @Test
    void returnedListDoesNotAliasCache() throws Exception {
        java.util.ArrayList<Multiaddr> src = new java.util.ArrayList<>(List.of(ADDR));
        HttpDelegatedRouting routing = routing(fetch(() -> CompletableFuture.completedFuture(src)));

        List<Multiaddr> first = routing.responsables(KEY).get(1, TimeUnit.SECONDS);

        src.add(SRC_EXTRA); // the source changed after the first fetch
        List<Multiaddr> second = routing.responsables(KEY).get(1, TimeUnit.SECONDS);

        assertEquals(List.of(ADDR), first, "caller must not see the source's mutable list");
        assertEquals(List.of(ADDR), second, "cache must not alias the source list (#9)");
        assertThrows(UnsupportedOperationException.class, first::clear,
                "callers must never be able to mutate the cached list");
    }

    private HttpDelegatedRouting routing(PeerSource source) {
        return new HttpDelegatedRouting(source, null, null, Duration.ofSeconds(60));
    }

    private PeerSource fetch(Supplier<CompletableFuture<List<Multiaddr>>> fn) {
        return new PeerSource() {
            @Override public CompletableFuture<List<Multiaddr>> fetch(PeerId self, int keyCount) {
                return CompletableFuture.completedFuture(List.of());
            }
            @Override public CompletableFuture<List<Multiaddr>> fetchForDhtKey(byte[] dhtKey) {
                return fn.get();
            }
            @Override public int getLookupKeys() { return 1; }
        };
    }
}