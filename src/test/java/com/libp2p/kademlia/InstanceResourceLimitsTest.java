package com.libp2p.kademlia;

import com.libp2p.kademlia.lookup.IterativeLookup;
import com.libp2p.kademlia.lookup.QueryScheduler;
import io.libp2p.core.PeerId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class InstanceResourceLimitsTest {

    private static final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "test-query");
        t.setDaemon(true);
        return t;
    });

    private IterativeLookup createLookup() {
        return new IterativeLookup(
                new byte[32], List.of(), 20, 3, 3,
                Duration.ofSeconds(10), null);
    }

    @Test
    void testIndependentActiveCounts() throws Exception {
        IterativeLookup lookupA = createLookup();
        IterativeLookup lookupB = createLookup();

        AtomicInteger counterA = new AtomicInteger(0);
        AtomicInteger counterB = new AtomicInteger(0);

        QueryScheduler schedulerA = new QueryScheduler(3, lookupA, id -> {
            counterA.incrementAndGet();
            CompletableFuture<Void> f = new CompletableFuture<>();
            executor.schedule(() -> f.complete(null), 200, TimeUnit.MILLISECONDS);
            return f;
        });

        QueryScheduler schedulerB = new QueryScheduler(3, lookupB, id -> {
            counterB.incrementAndGet();
            CompletableFuture<Void> f = new CompletableFuture<>();
            executor.schedule(() -> f.complete(null), 50, TimeUnit.MILLISECONDS);
            return f;
        });

        List<PeerId> peersA = new ArrayList<>();
        for (int i = 0; i < 5; i++) peersA.add(PeerId.random());
        schedulerA.submitPeers(peersA);

        Thread.sleep(50);

        assertTrue(counterA.get() > 0, "scheduler A should have dispatched queries");
        assertEquals(0, counterB.get(), "scheduler B should have 0 active count");

        List<PeerId> peersB = new ArrayList<>();
        for (int i = 0; i < 3; i++) peersB.add(PeerId.random());
        schedulerB.submitPeers(peersB);

        schedulerA.awaitCompletion().get(5, TimeUnit.SECONDS);
        schedulerB.awaitCompletion().get(5, TimeUnit.SECONDS);

        assertTrue(counterA.get() > 0, "scheduler A should have processed queries");
        assertTrue(counterB.get() > 0, "scheduler B should have processed queries");
    }

    @Test
    void testCancelOneDoesNotAffectOther() throws Exception {
        IterativeLookup lookupA = createLookup();
        IterativeLookup lookupB = createLookup();

        AtomicInteger counterA = new AtomicInteger(0);
        AtomicInteger counterB = new AtomicInteger(0);

        QueryScheduler schedulerA = new QueryScheduler(1, lookupA, id -> {
            counterA.incrementAndGet();
            CompletableFuture<Void> f = new CompletableFuture<>();
            executor.schedule(() -> f.complete(null), 100, TimeUnit.MILLISECONDS);
            return f;
        });

        QueryScheduler schedulerB = new QueryScheduler(1, lookupB, id -> {
            counterB.incrementAndGet();
            CompletableFuture<Void> f = new CompletableFuture<>();
            executor.schedule(() -> f.complete(null), 100, TimeUnit.MILLISECONDS);
            return f;
        });

        List<PeerId> peersA = new ArrayList<>();
        for (int i = 0; i < 10; i++) peersA.add(PeerId.random());
        schedulerA.submitPeers(peersA);

        List<PeerId> peersB = new ArrayList<>();
        for (int i = 0; i < 10; i++) peersB.add(PeerId.random());
        schedulerB.submitPeers(peersB);

        Thread.sleep(50);
        schedulerA.cancel();

        schedulerB.awaitCompletion().get(5, TimeUnit.SECONDS);

        assertTrue(counterB.get() >= 5,
                "scheduler B should continue processing after scheduler A cancel, got " + counterB.get());
    }

    @Test
    void testSchedulersHaveIndependentGlobalCounters() throws Exception {
        IterativeLookup lookupA = createLookup();
        IterativeLookup lookupB = createLookup();

        AtomicInteger counterA = new AtomicInteger(0);
        AtomicInteger counterB = new AtomicInteger(0);

        QueryScheduler schedulerA = new QueryScheduler(2, lookupA, id -> {
            counterA.incrementAndGet();
            CompletableFuture<Void> f = new CompletableFuture<>();
            executor.schedule(() -> f.complete(null), 200, TimeUnit.MILLISECONDS);
            return f;
        });

        QueryScheduler schedulerB = new QueryScheduler(2, lookupB, id -> {
            counterB.incrementAndGet();
            CompletableFuture<Void> f = new CompletableFuture<>();
            executor.schedule(() -> f.complete(null), 50, TimeUnit.MILLISECONDS);
            return f;
        });

        List<PeerId> peersA = new ArrayList<>();
        for (int i = 0; i < 4; i++) peersA.add(PeerId.random());
        schedulerA.submitPeers(peersA);

        Thread.sleep(50);

        int activeA = schedulerA.getGlobalActiveCount();
        int activeB = schedulerB.getGlobalActiveCount();

        assertTrue(activeA > 0, "scheduler A should have active queries, got " + activeA);
        assertEquals(0, activeB, "scheduler B global counter must be independent, should be 0");

        schedulerA.awaitCompletion().get(5, TimeUnit.SECONDS);
        schedulerB.awaitCompletion().get(5, TimeUnit.SECONDS);
    }
}
