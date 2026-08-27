package com.libp2p.kademlia;

import com.libp2p.kademlia.records.MemoryProviderStore;
import com.libp2p.kademlia.records.ProviderRecord;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ProviderStoreConcurrencyTest {

    private static final int THREADS = 32;
    private static final int KEYS = 8;

    private static byte[] key(int i) {
        return new byte[]{(byte) i, 0, 0, 1};
    }

    private static ProviderRecord record(byte[] key, PeerId provider, int tid) {
        Instant now = Instant.now();
        List<Multiaddr> addrs = List.of(
                Multiaddr.fromString("/ip4/10.0.0." + (tid % 255) + "/tcp/4001"));
        return new ProviderRecord(key, provider, now.plus(Duration.ofHours(48)),
                now.plus(Duration.ofMinutes(30)), addrs);
    }

    @Test
    void testConcurrentAddNoLostUpdates() throws Exception {
        MemoryProviderStore store = new MemoryProviderStore(1024, THREADS);
        List<List<PeerId>> threadPeers = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
            List<PeerId> peers = new ArrayList<>();
            for (int k = 0; k < KEYS; k++) peers.add(PeerId.random());
            threadPeers.add(peers);
        }

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        List<Future<?>> futures = new ArrayList<>();
        AtomicInteger errors = new AtomicInteger();

        for (int t = 0; t < THREADS; t++) {
            final int tid = t;
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    List<PeerId> peers = threadPeers.get(tid);
                    for (int k = 0; k < KEYS; k++) {
                        store.addProvider(record(key(k), peers.get(k), tid));
                    }
                } catch (Throwable e) {
                    errors.incrementAndGet();
                }
            }));
        }
        start.countDown();
        for (Future<?> f : futures) f.get(10, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(0, errors.get(), "no addProvider should throw");

        for (int k = 0; k < KEYS; k++) {
            List<ProviderRecord> providers = store.getProviders(key(k));
            Set<PeerId> providerIds = new java.util.HashSet<>();
            for (ProviderRecord r : providers) providerIds.add(r.getProvider());
            assertEquals(THREADS, providers.size(), "key " + k + " should retain all providers");
            for (int t = 0; t < THREADS; t++) {
                assertTrue(providerIds.contains(threadPeers.get(t).get(k)),
                        "key " + k + " missing provider from thread " + t);
            }
        }
    }

    @Test
    void testConcurrentReadWriteGcNoExceptions() throws Exception {
        MemoryProviderStore store = new MemoryProviderStore(1024, THREADS * 2);

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        List<Future<?>> futures = new ArrayList<>();
        AtomicInteger errors = new AtomicInteger();
        AtomicInteger gcCount = new AtomicInteger();

        for (int t = 0; t < THREADS; t++) {
            final int tid = t;
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int round = 0; round < 50; round++) {
                        int k = round % KEYS;
                        PeerId provider = PeerId.random();
                        store.addProvider(record(key(k), provider, tid));
                        List<ProviderRecord> got = store.getProviders(key(k));
                        for (ProviderRecord r : got) {
                            r.getAliveAddresses();
                            r.hashCode();
                        }
                        store.provided();
                        store.keyCount();
                        if (round % 7 == 0) gcCount.addAndGet(store.garbageCollect());
                    }
                } catch (Throwable e) {
                    errors.incrementAndGet();
                }
            }));
        }
        start.countDown();
        for (Future<?> f : futures) f.get(15, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(0, errors.get(), "no exceptions or ConcurrentModificationException during provider store stress");
    }

    @Test
    void testGarbageCollectionConcurrentDistinctKeys() throws Exception {
        MemoryProviderStore store = new MemoryProviderStore(1024, THREADS);
        int keys = 64;
        List<List<PeerId>> threadPeers = new ArrayList<>();
        for (int t = 0; t < THREADS; t++) {
            List<PeerId> peers = new ArrayList<>();
            for (int k = 0; k < keys; k++) peers.add(PeerId.random());
            threadPeers.add(peers);
        }

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        List<Future<?>> futures = new ArrayList<>();
        AtomicInteger errors = new AtomicInteger();

        for (int t = 0; t < THREADS; t++) {
            final int tid = t;
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int k = 0; k < keys; k++) {
                        store.addProvider(record(key(k % KEYS), threadPeers.get(tid).get(k), tid));
                        store.garbageCollect();
                    }
                } catch (Throwable e) {
                    errors.incrementAndGet();
                }
            }));
        }
        start.countDown();
        for (Future<?> f : futures) f.get(15, TimeUnit.SECONDS);
        pool.shutdown();

        assertEquals(0, errors.get(), "concurrent addProvider and garbageCollect must not throw");
    }
}
