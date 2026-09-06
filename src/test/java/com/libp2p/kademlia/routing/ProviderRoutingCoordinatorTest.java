package com.libp2p.kademlia.routing;

import com.libp2p.kademlia.records.ProviderRecord;
import io.libp2p.core.PeerId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Orchestration tests for {@link ProviderRoutingCoordinator}: how the native
 * kad path and the delegated path combine for provide/findProviders. No network
 * is touched — the paths are raw futures controlled by the test.
 */
class ProviderRoutingCoordinatorTest {

    private static final byte[] KEY = {1, 2, 3};
    private static final String PROVIDER_A = "12D3KooWHQnFuULtcGg9axasidmqVg8HHRw2XzKHPP8LjJpiwwHn";
    private static final String PROVIDER_B = "12D3KooWKnDdG3iXw9eTFijk3EWSunZcFi54Zka4wmtqtt6rPxc8";

    // short timeouts/grace so the suite stays fast; every test asserts on real completions
    private static final Duration TINY = Duration.ofMillis(50);
    private static final Duration GRACE = Duration.ofMillis(200);

    @Test
    void provideWithoutDelegatedReturnsKadResult() throws Exception {
        CompletableFuture<Boolean> ok = provideCoordinator(k -> CompletableFuture.completedFuture(true)).provide(KEY);
        assertEquals(true, ok.get(1, TimeUnit.SECONDS));

        CompletableFuture<Boolean> nope = provideCoordinator(k -> CompletableFuture.completedFuture(false)).provide(KEY);
        assertEquals(false, nope.get(1, TimeUnit.SECONDS));
    }

    @Test
    void provideTimesOutKadAndFailsWhenNoDelegated() {
        long t0 = System.nanoTime();
        CompletableFuture<Boolean> result = provideCoordinator(k -> new CompletableFuture<>()).provide(KEY);
        assertEquals(false, result.join());
        assertTrue((System.nanoTime() - t0) / 1_000_000 < 2000, "kad timeout must be bounded");
    }

    @Test
    void provideWinsOnDelegatedAckWithoutWaitingForKad() {
        long t0 = System.nanoTime();
        CompletableFuture<Boolean> result = provideCoordinator(
                k -> new CompletableFuture<>(),          // kad hangs forever
                delegated(true, List.of())).provide(KEY);
        assertEquals(true, result.join());
        assertTrue((System.nanoTime() - t0) / 1_000_000 < 1000, "delegated ack must return without waiting for kad");
    }

    @Test
    void provideWinsOnKadSuccessWhenDelegatedSilent() {
        CompletableFuture<Boolean> result = provideCoordinator(
                k -> CompletableFuture.completedFuture(true),
                delegated(false, List.of())).provide(KEY);
        assertEquals(true, result.join());
    }

    @Test
    void provideFailsWhenBothPathsFail() {
        CompletableFuture<Boolean> result = provideCoordinator(
                k -> CompletableFuture.completedFuture(false),
                delegated(false, List.of())).provide(KEY);
        assertEquals(false, result.join());
    }

    @Test
    void findWithoutDelegatedReturnsKadResult() throws Exception {
        List<ProviderRecord> kadOnly = List.of(record(PROVIDER_A));
        CompletableFuture<List<ProviderRecord>> result = findCoordinator(
                k -> CompletableFuture.completedFuture(kadOnly)).findProviders(KEY);
        assertEquals(kadOnly, result.get(1, TimeUnit.SECONDS));
    }

    @Test
    void findReturnsKadResultWhenDelegatedArrivesAfterGrace() {
        // delegated responds outside the grace window: only the first path's answer survives
        CompletableFuture<List<ProviderRecord>> result = findCoordinator(
                k -> CompletableFuture.completedFuture(List.of(record(PROVIDER_A))),
                delegatedProvidersAfter(List.of(record(PROVIDER_B)), GRACE.toMillis() * 4))
                .findProviders(KEY);
        List<ProviderRecord> got = result.join();
        assertEquals(List.of(PROVIDER_A), got.stream().map(p -> p.getProvider().toBase58()).toList());
    }

    @Test
    void findMergesDelegatedResultArrivingWithinGrace() {
        CompletableFuture<List<ProviderRecord>> result = findCoordinator(
                k -> CompletableFuture.completedFuture(List.of(record(PROVIDER_A))),
                delegatedProvidersAfter(List.of(record(PROVIDER_B)), TINY.toMillis() * 2))
                .findProviders(KEY);
        List<ProviderRecord> got = result.join();
        assertEquals(2, got.size());
        assertTrue(got.stream().anyMatch(p -> p.getProvider().toBase58().equals(PROVIDER_A)));
        assertTrue(got.stream().anyMatch(p -> p.getProvider().toBase58().equals(PROVIDER_B)));
    }

    @Test
    void findDeduplicatesProvidersAcrossPaths() {
        CompletableFuture<List<ProviderRecord>> result = findCoordinator(
                k -> CompletableFuture.completedFuture(List.of(record(PROVIDER_A))),
                delegatedProvidersAfter(List.of(record(PROVIDER_A), record(PROVIDER_B)), TINY.toMillis() * 2))
                .findProviders(KEY);
        List<ProviderRecord> got = result.join();
        assertEquals(List.of(PROVIDER_A, PROVIDER_B), got.stream().map(p -> p.getProvider().toBase58()).toList());
    }

    @Test
    void findReturnsEmptyWhenBothPathsEmpty() {
        CompletableFuture<List<ProviderRecord>> result = findCoordinator(
                k -> CompletableFuture.completedFuture(List.of()),
                delegated(List.of()))
                .findProviders(KEY);
        assertTrue(result.join().isEmpty());
    }

    private ProviderRoutingCoordinator provideCoordinator(Function<byte[], CompletableFuture<Boolean>> kad) {
        return new ProviderRoutingCoordinator(
                kad,
                k -> CompletableFuture.completedFuture(List.of()),
                null, TINY, TINY, GRACE);
    }

    private ProviderRoutingCoordinator provideCoordinator(
            Function<byte[], CompletableFuture<Boolean>> kad,
            DelegatedRouting delegated) {
        return new ProviderRoutingCoordinator(
                kad,
                k -> CompletableFuture.completedFuture(List.of()),
                delegated, TINY, TINY, GRACE);
    }

    private ProviderRoutingCoordinator findCoordinator(
            Function<byte[], CompletableFuture<List<ProviderRecord>>> kad) {
        return new ProviderRoutingCoordinator(
                k -> CompletableFuture.completedFuture(false),
                kad, null, TINY, TINY, GRACE);
    }

    private ProviderRoutingCoordinator findCoordinator(
            Function<byte[], CompletableFuture<List<ProviderRecord>>> kad,
            DelegatedRouting delegated) {
        return new ProviderRoutingCoordinator(
                k -> CompletableFuture.completedFuture(false),
                kad, delegated, TINY, TINY, GRACE);
    }

    private DelegatedRouting delegated(boolean announceResult, List<ProviderRecord> providers) {
        return new DelegatedRouting() {
            @Override public CompletableFuture<Boolean> announce(byte[] key) {
                return CompletableFuture.completedFuture(announceResult);
            }

            @Override public CompletableFuture<List<ProviderRecord>> findProviders(byte[] key) {
                return CompletableFuture.completedFuture(providers);
            }
        };
    }

    private DelegatedRouting delegated(List<ProviderRecord> providers) {
        return delegated(false, providers);
    }

    private DelegatedRouting delegatedProvidersAfter(List<ProviderRecord> providers, long delayMillis) {
        return new DelegatedRouting() {
            @Override public CompletableFuture<Boolean> announce(byte[] key) {
                return CompletableFuture.completedFuture(false);
            }

            @Override public CompletableFuture<List<ProviderRecord>> findProviders(byte[] key) {
                CompletableFuture<List<ProviderRecord>> f = new CompletableFuture<>();
                Thread t = new Thread(() -> {
                    try { Thread.sleep(delayMillis); } catch (InterruptedException ignored) {}
                    f.complete(providers);
                });
                t.setDaemon(true);
                t.start();
                return f;
            }
        };
    }

    private static ProviderRecord record(String peerBase58) {
        return new ProviderRecord(KEY, PeerId.fromBase58(peerBase58),
                Instant.now().plusSeconds(60), Instant.now().plusSeconds(60), List.of());
    }
}