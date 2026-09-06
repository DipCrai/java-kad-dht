package com.libp2p.kademlia;

import com.libp2p.kademlia.config.KadConfig;
import com.libp2p.kademlia.config.KadMode;
import com.libp2p.kademlia.protocol.KademliaProtocol;
import com.libp2p.kademlia.records.ProviderRecord;
import io.libp2p.core.Host;
import io.libp2p.core.crypto.KeyKt;
import io.libp2p.core.crypto.KeyType;
import io.libp2p.core.crypto.PubKey;
import io.libp2p.core.crypto.PrivKey;
import io.libp2p.core.dsl.Builder;
import io.libp2p.core.dsl.BuilderJKt;
import io.libp2p.core.mux.StreamMuxerProtocol;
import io.libp2p.protocol.IdentifyBinding;
import io.libp2p.protocol.IdentifyProtocol;
import io.libp2p.protocol.PingBinding;
import io.libp2p.protocol.PingProtocol;
import io.libp2p.security.noise.NoiseXXSecureChannel;

import java.security.MessageDigest;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end provider round-trip over the real public libp2p DHT, mirroring the
 * Peerfect mod: B knows A's friend public key, uses SHA-256(pubKey) as content
 * key, A provides on it, B findProviders(). Run from the machine that will act
 * as a Peerfect peer: <pre>
 *   ./gradlew kadHttpE2EProbe                  (default listen /ip4/192.168.0.112/tcp/0)
 *   ./gradlew kadHttpE2EProbe -Dkad.listen=/ip4/x.x.x.x/tcp/0
 * </pre>
 */
public class HttpBootstrapE2EProbe {

    static final String LISTEN_ADDR = System.getProperty("kad.listen", "/ip4/192.168.0.112/tcp/0");

    public static void main(String[] args) throws Exception {
        PrivKey keyA = KeyKt.generateKeyPair(KeyType.ED25519).getFirst();
        PrivKey keyB = KeyKt.generateKeyPair(KeyType.ED25519).getFirst();
        PubKey pubA = keyA.publicKey();

        byte[] friendKey = sha256(pubA.raw());
        System.out.println("listen: " + LISTEN_ADDR);

        Node nodeA = startNode("A", keyA);
        Node nodeB = startNode("B", keyB);

        try {
            long t0 = System.nanoTime();
            try { nodeA.dht.bootstrap().get(10, TimeUnit.SECONDS); } catch (Exception ignored) {}
            waitForRT(nodeA, "A", t0);
            System.out.println("[A] bootstrap RT=" + nodeA.dht.getRoutingTable().size());

            t0 = System.nanoTime();
            try { nodeB.dht.bootstrap().get(10, TimeUnit.SECONDS); } catch (Exception ignored) {}
            waitForRT(nodeB, "B", t0);
            System.out.println("[B] bootstrap RT=" + nodeB.dht.getRoutingTable().size());

            long tProvide = System.nanoTime();
            Boolean provided = nodeA.dht.provide(friendKey).get(60, TimeUnit.SECONDS);
            System.out.println("[A] provide(key) -> " + provided + " in " + ms(tProvide) + " ms");

            String verdict = "FAIL";
            for (int round = 1; round <= 3; round++) {
                Thread.sleep(3000);
                long tr = System.nanoTime();
                List<ProviderRecord> providers = nodeB.dht.findProviders(friendKey).get(60, TimeUnit.SECONDS);
                boolean found = providers.stream().anyMatch(p -> p.getProvider().toBase58().equals(nodeA.peerId.toBase58()));
                System.out.println("[B] round " + round + ": findProviders -> " + providers.size() + " records, provider==A? " + found + " in " + ms(tr) + " ms");
                if (found) { verdict = "OK"; break; }
            }
            System.out.println("E2E PROVIDER ROUND-TRIP: " + verdict);
            if (verdict.equals("OK")) {
                System.out.println("[E2E] provide -> found: " + ms(tProvide) + " ms total (includes 3s poll gap)");
            }
        } finally {
            nodeA.dht.close(); nodeA.host.stop().get(10, TimeUnit.SECONDS);
            nodeB.dht.close(); nodeB.host.stop().get(10, TimeUnit.SECONDS);
        }
        System.exit(0);
    }

    private static void waitForRT(Node node, String tag, long t0) throws Exception {
        while (node.dht.getRoutingTable().size() == 0 && (System.nanoTime() - t0) / 1_000_000 < 40_000) {
            Thread.sleep(1000);
        }
        System.out.println("[" + tag + "] bootstrap done in " + ms(t0) + " ms");
    }

    private static Node startNode(String tag, PrivKey key) throws Exception {
        Host host = BuilderJKt.hostJ(Builder.Defaults.Standard, b -> {
            b.getNetwork().getListen().clear();
            b.getNetwork().getListen().add(LISTEN_ADDR);
            b.getIdentity().setFactory(() -> key);
            b.getMuxers().clear();
            b.getMuxers().add(StreamMuxerProtocol.getYamux());
            b.getSecureChannels().add((privKey, muxers) -> new NoiseXXSecureChannel(privKey));
            b.getProtocols().add(new IdentifyBinding(new IdentifyProtocol()));
            b.getProtocols().add(new PingBinding(new PingProtocol()));
        });
        host.start().get(10, TimeUnit.SECONDS);

        KadConfig config = KadConfig.builder()
                .mode(KadMode.AUTO_SERVER)
                .httpBootstrapFallback(true)
                .queryTimeout(Duration.ofSeconds(90))
                .substreamTimeout(Duration.ofSeconds(15))
                .build();
        KadDht dht = new KadDht(config);
        dht.setHost(host);
        dht.start().get(15, TimeUnit.SECONDS);
        System.out.println("[" + tag + "] peer=" + host.getPeerId().toBase58() + " addr=" + host.listenAddresses());
        return new Node(host, dht);
    }

    private static byte[] sha256(byte[] data) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(data);
    }

    private static long ms(long t0) {
        return (System.nanoTime() - t0) / 1_000_000;
    }

    private static class Node {
        final Host host;
        final KadDht dht;
        final io.libp2p.core.PeerId peerId;
        Node(Host host, KadDht dht) {
            this.host = host;
            this.dht = dht;
            this.peerId = host.getPeerId();
        }
    }
}