package com.libp2p.kademlia;

import com.libp2p.kademlia.lookup.IterativeLookup;
import com.libp2p.kademlia.lookup.QueryScheduler;
import io.libp2p.core.PeerId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class QuerySchedulerTest {

    private IterativeLookup createLookup() {
        return new IterativeLookup(
                new byte[32], List.of(), 20, 3, 3,
                Duration.ofSeconds(10), null);
    }

    @Test
    void testDispatch() throws Exception {
        IterativeLookup lookup = createLookup();
        AtomicInteger counter = new AtomicInteger(0);
        QueryScheduler scheduler = new QueryScheduler(3, lookup, id -> {
            counter.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        });

        List<PeerId> peers = new ArrayList<>();
        for (int i = 0; i < 5; i++) peers.add(PeerId.random());

        scheduler.submitPeers(peers);
        scheduler.awaitCompletion().get(2, TimeUnit.SECONDS);

        assertTrue(counter.get() >= 1, "at least one query should have been dispatched");
    }

    @Test
    void testAlphaLimit() throws Exception {
        IterativeLookup lookup = createLookup();
        AtomicInteger maxConcurrent = new AtomicInteger(0);
        AtomicInteger currentConcurrent = new AtomicInteger(0);

        QueryScheduler scheduler = new QueryScheduler(2, lookup, id -> {
            int c = currentConcurrent.incrementAndGet();
            int prevMax;
            do { prevMax = maxConcurrent.get(); }
            while (c > prevMax && !maxConcurrent.compareAndSet(prevMax, c));
            try { Thread.sleep(50); } catch (InterruptedException ignored) {}
            currentConcurrent.decrementAndGet();
            return CompletableFuture.completedFuture(null);
        });

        List<PeerId> peers = new ArrayList<>();
        for (int i = 0; i < 10; i++) peers.add(PeerId.random());

        scheduler.submitPeers(peers);
        scheduler.awaitCompletion().get(5, TimeUnit.SECONDS);

        assertTrue(maxConcurrent.get() <= 2, "should not exceed alpha limit of 2, was " + maxConcurrent.get());
    }

    @Test
    void testCancel() throws Exception {
        IterativeLookup lookup = createLookup();
        AtomicInteger counter = new AtomicInteger(0);

        QueryScheduler scheduler = new QueryScheduler(1, lookup, id -> {
            counter.incrementAndGet();
            try { Thread.sleep(200); } catch (InterruptedException ignored) {}
            return CompletableFuture.completedFuture(null);
        });

        List<PeerId> peers = new ArrayList<>();
        for (int i = 0; i < 20; i++) peers.add(PeerId.random());

        scheduler.submitPeers(peers);
        scheduler.cancel();

        CompletableFuture<Void> result = scheduler.awaitCompletion();
        assertDoesNotThrow(() -> result.get(2, TimeUnit.SECONDS));
        assertTrue(counter.get() <= 20, "not all peers should be processed after cancel");
    }

    @Test
    void testNewPeerDiscovery() throws Exception {
        IterativeLookup lookup = createLookup();
        AtomicInteger counter = new AtomicInteger(0);
        java.util.concurrent.atomic.AtomicReference<QueryScheduler> ref = new java.util.concurrent.atomic.AtomicReference<>();

        QueryScheduler scheduler = new QueryScheduler(1, lookup, id -> {
            counter.incrementAndGet();
            if (counter.get() == 1) {
                QueryScheduler s = ref.get();
                if (s != null) s.submitPeers(List.of(PeerId.random()));
            }
            return CompletableFuture.completedFuture(null);
        });
        ref.set(scheduler);

        scheduler.submitPeers(List.of(PeerId.random()));
        scheduler.awaitCompletion().get(2, TimeUnit.SECONDS);

        assertTrue(counter.get() >= 2, "newly discovered peers should be dispatched");
    }
}
