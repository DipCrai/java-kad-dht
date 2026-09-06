package com.libp2p.kademlia;

import com.libp2p.kademlia.config.KadConfig;
import com.libp2p.kademlia.config.KadMode;
import io.libp2p.core.Host;
import io.libp2p.core.crypto.KeyKt;
import io.libp2p.core.crypto.KeyType;
import io.libp2p.core.crypto.PrivKey;
import io.libp2p.core.dsl.Builder;
import io.libp2p.core.dsl.BuilderJKt;
import io.libp2p.core.mux.StreamMuxerProtocol;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.protocol.IdentifyBinding;
import io.libp2p.protocol.IdentifyProtocol;
import io.libp2p.protocol.PingBinding;
import io.libp2p.protocol.PingProtocol;
import io.libp2p.security.noise.NoiseXXSecureChannel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HttpBootstrapFallbackProbe {

    public static void main(String[] args) throws Exception {
        PrivKey key = KeyKt.generateKeyPair(KeyType.ED25519).getFirst();
        Host host = BuilderJKt.hostJ(Builder.Defaults.Standard, b -> {
            b.getNetwork().getListen().clear();
            b.getNetwork().getListen().add("/ip4/127.0.0.1/tcp/0");
            b.getIdentity().setFactory(() -> key);
            b.getMuxers().clear();
            b.getMuxers().add(StreamMuxerProtocol.getYamux());
            b.getSecureChannels().add((privKey, muxers) -> new NoiseXXSecureChannel(privKey));
            b.getProtocols().add(new IdentifyBinding(new IdentifyProtocol()));
            b.getProtocols().add(new PingBinding(new PingProtocol()));
        });
        host.start().get(10, TimeUnit.SECONDS);
        System.out.println("local peer: " + host.getPeerId().toBase58());
        System.out.println("local addr: " + host.listenAddresses());

        boolean emptyBootstrap = Boolean.parseBoolean(System.getProperty("kad.empty", "false"));
        var cfgBuilder = KadConfig.builder()
                .mode(KadMode.AUTO_SERVER)
                .httpBootstrapFallback(true)
                .queryTimeout(Duration.ofSeconds(90))
                .substreamTimeout(Duration.ofSeconds(15));
        if (emptyBootstrap) {
            cfgBuilder = cfgBuilder.bootstrapNodes(List.of());
            System.out.println("bootstrap nodes: EMPTY (HTTP is primary)");
        } else {
            System.out.println("bootstrap nodes: default officials (" + KadConfig.builder().build().getBootstrapNodes().size() + "), HTTP runs in parallel");
        }

        KadDht dht = new KadDht(cfgBuilder.build());
        dht.setHost(host);
        dht.start().get(15, TimeUnit.SECONDS);

        var source = com.libp2p.kademlia.refresh.HttpBootstrapPeerSource.defaults();
        var fetched = source.fetch(host.getPeerId(), 3).get(30, TimeUnit.SECONDS);
        System.out.println("fetch: " + fetched.size() + " candidate addrs");
        for (var m : fetched.subList(0, Math.min(3, fetched.size()))) {
            System.out.println("  cand: " + m);
        }

        long t0 = System.nanoTime();
        try {
            dht.bootstrap().get(60, TimeUnit.SECONDS);
            System.out.println("bootstrap(): SUCCEEDED in " + ((System.nanoTime() - t0) / 1_000_000) + " ms");
        } catch (Exception e) {
            System.out.println("bootstrap(): " + e);
        }
        Thread.sleep(6000);

        System.out.println("routing table size: " + dht.getRoutingTable().size());
        System.out.println("non-empty buckets:  " + dht.getRoutingTable().nonEmptyBuckets());
        System.out.println("bucket occupancies: " + dht.getRoutingTable().getAverageBucketOccupancy());

        byte[] keyB = new byte[32];
        java.util.Arrays.fill(keyB, (byte) 0xFF);
        int ok = 0, bad = 0;
        for (var kp : dht.getRoutingTable().getAllPeers()) {
            long s = System.nanoTime();
            try {
                var r = dht.getProtocol().sendFindNode(keyB, kp).get(10, TimeUnit.SECONDS);
                ok++;
                System.out.println("  KAD-OK   " + kp.toBase58() + " closer=" + r.closerPeers().size() + " [" + ((System.nanoTime() - s) / 1_000_000) + "ms]");
            } catch (Exception e) {
                bad++;
                System.out.println("  KAD-BAD  " + kp.toBase58() + " [" + ((System.nanoTime() - s) / 1_000_000) + "ms] " + e.getClass().getSimpleName());
            }
        }
        System.out.println("kad check: ok=" + ok + " bad=" + bad);

        try {
            var prov = dht.findProviders(keyB).get(90, TimeUnit.SECONDS);
            System.out.println("findProviders returned: " + prov.size());
        } catch (Exception e) {
            System.out.println("findProviders: " + e);
        }

        System.out.println("FALLBACK PROBE DONE");
        dht.close();
        host.stop().get(10, TimeUnit.SECONDS);
        System.exit(0);
    }
}