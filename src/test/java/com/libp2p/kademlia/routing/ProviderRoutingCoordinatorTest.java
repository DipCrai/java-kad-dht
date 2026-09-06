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
    private static final String PROVIDER_C = "12D3KooWPK7CkDkM6PKQY5gV4qpzX5mFhFNZHhtdKx8L9xRdCvG7";

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

    // ---- failure matrix (#13) -------------------------------------------------

    @Test
    void provideKadThrowsImmediatelyFailsWithoutDelegated() {
        CompletableFuture<Boolean> result = provideCoordinator(k -> failingProvide()).provide(KEY);
        assertEquals(false, result.join());
    }

    @Test
    void provideKadThrowsButDelegatedWins() {
        CompletableFuture<Boolean> result = provideCoordinator(k -> failingProvide(), delegated(true, List.of())).provide(KEY);
        assertEquals(true, result.join());
    }

    @Test
    void provideDelegatedThrowsButKadWins() {
        CompletableFuture<Boolean> result = provideCoordinator(
                k -> CompletableFuture.completedFuture(true),
                throwingDelegated()).provide(KEY);
        assertEquals(true, result.join());
    }

    @Test
    void provideBothThrowFails() {
        CompletableFuture<Boolean> result = provideCoordinator(
                k -> failingProvide(),
                throwingDelegated()).provide(KEY);
        assertEquals(false, result.join());
    }

    @Test
    void provideKadThrowsAndDelegatedEmptyFails() {
        // "one throws + other empty" must not mask the failure as a win
        CompletableFuture<Boolean> result = provideCoordinator(
                k -> failingProvide(),
                delegated(false, List.of())).provide(KEY);
        assertEquals(false, result.join());
    }

    @Test
    void provideKadAnnounceResultQuorumReachedCountsAsSuccess() {
        // native path reports a partial write whose quorum was reached (#3/#4)
        CompletableFuture<Boolean> result = provideCoordinator(
                k -> CompletableFuture.completedFuture(new ProviderRoutingCoordinator.AnnounceResult(true, 5, 3)),
                delegated(false, List.of())).provide(KEY);
        assertEquals(true, result.join());
    }

    @Test
    void provideKadAnnounceResultNoQuorumCountsAsFailure() {
        CompletableFuture<Boolean> result = provideCoordinator(
                k -> CompletableFuture.completedFuture(new ProviderRoutingCoordinator.AnnounceResult(false, 5, 1)),
                delegated(false, List.of())).provide(KEY);
        assertEquals(false, result.join());
    }

    @Test
    void findKadThrowsReturnsDelegated() {
        CompletableFuture<List<ProviderRecord>> result = findCoordinator(
                k -> failingFind(),
                delegated(true, List.of(record(PROVIDER_B)))).findProviders(KEY);
        assertEquals(List.of(PROVIDER_B), result.join().stream().map(p -> p.getProvider().toBase58()).toList());
    }

    @Test
    void findDelegatedThrowsReturnsKad() {
        CompletableFuture<List<ProviderRecord>> result = findCoordinator(
                k -> CompletableFuture.completedFuture(List.of(record(PROVIDER_A))),
                throwingDelegated()).findProviders(KEY);
        assertEquals(List.of(PROVIDER_A), result.join().stream().map(p -> p.getProvider().toBase58()).toList());
    }

    @Test
    void findBothThrowReturnsEmpty() {
        CompletableFuture<List<ProviderRecord>> result = findCoordinator(
                k -> failingFind(),
                throwingDelegated()).findProviders(KEY);
        assertTrue(result.join().isEmpty());
    }

    @Test
    void findKadThrowsAndDelegatedEmptyReturnsEmpty() {
        CompletableFuture<List<ProviderRecord>> result = findCoordinator(
                k -> failingFind(),
                delegated(List.of())).findProviders(KEY);
        assertTrue(result.join().isEmpty());
    }

    @Test
    void findBothNonEmptySimultaneouslyMerges() {
        CompletableFuture<List<ProviderRecord>> result = findCoordinator(
                k -> CompletableFuture.completedFuture(List.of(record(PROVIDER_A))),
                new DelegatedRouting() {
                    @Override public CompletableFuture<Boolean> announce(byte[] key) {
                        return CompletableFuture.completedFuture(true);
                    }
                    @Override public CompletableFuture<List<ProviderRecord>> findProviders(byte[] key) {
                        // answers in the same instant as kad
                        return CompletableFuture.completedFuture(List.of(record(PROVIDER_B)));
                    }
                }).findProviders(KEY);
        List<ProviderRecord> got = result.join();
        assertEquals(2, got.size());
    }

    @Test
    void findDuplicatesFromBothPathsAreDeduplicated() {
        CompletableFuture<List<ProviderRecord>> result = findCoordinator(
                k -> CompletableFuture.completedFuture(List.of(record(PROVIDER_A), record(PROVIDER_B))),
                new DelegatedRouting() {
                    @Override public CompletableFuture<Boolean> announce(byte[] key) {
                        return CompletableFuture.completedFuture(true);
                    }
                    @Override public CompletableFuture<List<ProviderRecord>> findProviders(byte[] key) {
                        return CompletableFuture.completedFuture(List.of(record(PROVIDER_A), record(PROVIDER_C)));
                    }
                }).findProviders(KEY);
        List<String> got = result.join().stream().map(p -> p.getProvider().toBase58()).toList();
        assertEquals(3, got.size());
        assertEquals(java.util.Set.of(PROVIDER_A, PROVIDER_B, PROVIDER_C), java.util.Set.copyOf(got));
    }

    @Test
    void findDelegatedArrivingExactlyAtGraceBoundaryEitherMatches() throws Exception {
        // completion lands exactly on the grace deadline; both "merged" and
        // "first only" are valid outcomes, neither may be an exception or a block
        CompletableFuture<List<ProviderRecord>> result = findCoordinator(
                k -> CompletableFuture.completedFuture(List.of(record(PROVIDER_A))),
                delegatedProvidersAfter(List.of(record(PROVIDER_B)), GRACE.toMillis())).findProviders(KEY);
        List<ProviderRecord> got = result.get(2, TimeUnit.SECONDS);
        assertTrue(got.size() == 1 || got.size() == 2);
        assertTrue(got.stream().anyMatch(p -> p.getProvider().toBase58().equals(PROVIDER_A)));
    }

    @Test
    void tenConcurrentProvidesAllResolveConsistently() throws Exception {
        ProviderRoutingCoordinator coordinator = provideCoordinator(
                k -> CompletableFuture.completedFuture(true),
                delegated(true, List.of()));
        java.util.List<CompletableFuture<Boolean>> all = new java.util.ArrayList<>();
        for (int i = 0; i < 10; i++) all.add(coordinator.provide(KEY));
        for (CompletableFuture<Boolean> f : all) assertEquals(true, f.get(2, TimeUnit.SECONDS));
    }

    @Test
    void provideKadFalseAndHangingDelegatedFailsWithinTimeout() throws Exception {
        // delegated announce never completes: the coordinator must resolve by its
        // own provide timeout instead of waiting on anySucceeded forever
        long t0 = System.nanoTime();
        CompletableFuture<Boolean> result = provideCoordinator(
                k -> CompletableFuture.completedFuture(false),
                hangingDelegated()).provide(KEY);
        assertEquals(false, result.get(2, TimeUnit.SECONDS));
        assertTrue((System.nanoTime() - t0) / 1_000_000 < 2000, "hanging delegated must be cut by the provide timeout");
    }

    @Test
    void findEmptyKadAndHangingDelegatedReturnsEmptyWithinGrace() throws Exception {
        // empty first answer must still arm the grace deadline, so a hanging
        // delegated branch cannot block the caller forever
        long t0 = System.nanoTime();
        CompletableFuture<List<ProviderRecord>> result = findCoordinator(
                k -> CompletableFuture.completedFuture(List.of()),
                hangingDelegated()).findProviders(KEY);
        assertTrue(result.get(2, TimeUnit.SECONDS).isEmpty());
        assertTrue((System.nanoTime() - t0) / 1_000_000 < 2000,
                "empty-and-hanging must be bounded by the grace deadline");
    }

    @Test
    void tenConcurrentFindsAllResolveConsistently() throws Exception {
        ProviderRoutingCoordinator coordinator = findCoordinator(
                k -> CompletableFuture.completedFuture(List.of(record(PROVIDER_A))),
                new DelegatedRouting() {
                    @Override public CompletableFuture<Boolean> announce(byte[] key) {
                        return CompletableFuture.completedFuture(true);
                    }
                    @Override public CompletableFuture<List<ProviderRecord>> findProviders(byte[] key) {
                        return CompletableFuture.completedFuture(List.of(record(PROVIDER_B)));
                    }
                });
        java.util.List<CompletableFuture<List<ProviderRecord>>> all = new java.util.ArrayList<>();
        for (int i = 0; i < 10; i++) all.add(coordinator.findProviders(KEY));
        for (CompletableFuture<List<ProviderRecord>> f : all) {
            assertEquals(2, f.get(2, TimeUnit.SECONDS).size());
        }
    }

    private CompletableFuture<Boolean> failingProvide() {
        CompletableFuture<Boolean> f = new CompletableFuture<>();
        f.completeExceptionally(new IllegalStateException("kad path crash"));
        return f;
    }

    private CompletableFuture<List<ProviderRecord>> failingFind() {
        CompletableFuture<List<ProviderRecord>> f = new CompletableFuture<>();
        f.completeExceptionally(new IllegalStateException("kad path crash"));
        return f;
    }

    private DelegatedRouting throwingDelegated() {
        return new DelegatedRouting() {
            @Override public CompletableFuture<Boolean> announce(byte[] key) {
                CompletableFuture<Boolean> f = new CompletableFuture<>();
                f.completeExceptionally(new IllegalStateException("http path crash"));
                return f;
            }
            @Override public CompletableFuture<List<ProviderRecord>> findProviders(byte[] key) {
                CompletableFuture<List<ProviderRecord>> f = new CompletableFuture<>();
                f.completeExceptionally(new IllegalStateException("http path crash"));
                return f;
            }
        };
    }

    private DelegatedRouting hangingDelegated() {
        return new DelegatedRouting() {
            @Override public CompletableFuture<Boolean> announce(byte[] key) {
                return new CompletableFuture<>();
            }
            @Override public CompletableFuture<List<ProviderRecord>> findProviders(byte[] key) {
                return new CompletableFuture<>();
            }
        };
    }

    private ProviderRoutingCoordinator provideCoordinator(Function<byte[], CompletableFuture<?>> kad) {
        return new ProviderRoutingCoordinator(
                kad,
                k -> CompletableFuture.completedFuture(List.of()),
                null, TINY, TINY, GRACE);
    }

    private ProviderRoutingCoordinator provideCoordinator(
            Function<byte[], CompletableFuture<?>> kad,
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