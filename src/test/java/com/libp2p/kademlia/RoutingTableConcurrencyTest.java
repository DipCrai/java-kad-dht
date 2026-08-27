package com.libp2p.kademlia;

import com.libp2p.kademlia.routing.KBucketEntry;
import com.libp2p.kademlia.routing.RoutingTable;
import com.libp2p.kademlia.routing.KadPeer;
import io.libp2p.core.PeerId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RoutingTableConcurrencyTest {

    @Test
    void testConcurrentInsertDistinctPeersNoLoss() throws Exception {
        int k = 200;
        int threads = 200;
        PeerId local = PeerId.random();
        RoutingTable rt = new RoutingTable(local, k, Duration.ofSeconds(60));
        List<PeerId> inserted = new ArrayList<>();
        for (int i = 0; i < threads; i++) inserted.add(PeerId.random());

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>();
        AtomicInteger errors = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            final PeerId p = inserted.get(i);
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    rt.insert(p, List.of());
                } catch (Throwable e) {
                    errors.incrementAndGet();
                }
            }));
        }
        start.countDown();
        for (Future<?> f : futures) f.get(15, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(0, errors.get(), "no insert should throw");
        assertEquals(threads, rt.size(), "every distinct peer must be retained as an entry");

        for (PeerId p : inserted) {
            List<KadPeer> closest = rt.findClosest(XorId.fromPeerId(p), k);
            assertTrue(closest.stream().anyMatch(kp -> kp.nodeId.equals(p)),
                    "inserted peer " + p + " must be findable via findClosest");
        }
        assertNoDuplicates(rt);
    }

    @Test
    void testConcurrentInsertRemoveSamePeersNoLoss() throws Exception {
        int k = 100;
        PeerId local = PeerId.random();
        RoutingTable rt = new RoutingTable(local, k, Duration.ofSeconds(60));
        List<PeerId> shared = new ArrayList<>();
        for (int i = 0; i < 50; i++) shared.add(PeerId.random());

        int threads = 100;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>();
        AtomicInteger errors = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int round = 0; round < 50; round++) {
                        PeerId p = shared.get((tid + round) % shared.size());
                        rt.insert(p, List.of());
                        if (round % 2 == 0) {
                            rt.remove(p);
                        }
                    }
                } catch (Throwable e) {
                    errors.incrementAndGet();
                }
            }));
        }
        start.countDown();
        for (Future<?> f : futures) f.get(15, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(0, errors.get(), "no insert/remove should throw");

        for (PeerId p : shared) rt.insert(p, List.of());

        assertEquals(shared.size(), rt.size(), "all shared peers must be present after re-insert");
        for (PeerId p : shared) {
            List<KadPeer> closest = rt.findClosest(XorId.fromPeerId(p), k);
            assertTrue(closest.stream().anyMatch(kp -> kp.nodeId.equals(p)),
                    "peer " + p + " must be findable after re-insert");
        }
        assertNoDuplicates(rt);
    }

    @Test
    void testSimultaneousOverflowSingleBucket() throws Exception {
        int k = 20;
        PeerId local = PeerId.random();
        RoutingTable rt = new RoutingTable(local, k, Duration.ofSeconds(60));

        int threads = 100;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>();
        AtomicInteger errors = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < 5; i++) {
                        rt.insert(PeerId.random(), List.of());
                    }
                } catch (Throwable e) {
                    errors.incrementAndGet();
                }
            }));
        }
        start.countDown();
        for (Future<?> f : futures) f.get(15, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(0, errors.get(), "concurrent single-bucket overflow must not throw");
        assertNoDuplicates(rt);

        for (int i = 0; i < rt.getBucketCount(); i++) {
            assertTrue(rt.getBucket(i).size() <= k, "bucket " + i + " must never exceed capacity k");
        }

        assertEquals(k, rt.getBucket(0).size(), "busiest bucket must reach full capacity");
        assertEquals(k, rt.getBucket(0).getReplacementCache().size(),
                "busiest bucket replacement cache must fill to capacity");
    }

    @Test
    void testNoDuplicateEntriesAcrossConcurrentDuplicateInserts() throws Exception {
        PeerId local = PeerId.random();
        RoutingTable rt = new RoutingTable(local, 5, Duration.ofSeconds(60));
        List<PeerId> peers = new ArrayList<>();
        for (int i = 0; i < 40; i++) peers.add(PeerId.random());

        int threads = 40;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>();
        AtomicInteger errors = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            final PeerId p = peers.get(i);
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int rep = 0; rep < 10; rep++) {
                        rt.insert(p, List.of());
                    }
                } catch (Throwable e) {
                    errors.incrementAndGet();
                }
            }));
        }
        start.countDown();
        for (Future<?> f : futures) f.get(15, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(0, errors.get(), "concurrent duplicate inserts must not throw");
        assertNoDuplicates(rt);
    }

    private void assertNoDuplicates(RoutingTable rt) {
        Set<PeerId> allFound = new HashSet<>();
        for (int i = 0; i < rt.getBucketCount(); i++) {
            for (KBucketEntry entry : rt.getBucket(i).getEntries()) {
                assertTrue(allFound.add(entry.peerId), "duplicate peer " + entry.peerId + " found across buckets");
            }
        }
    }
}
