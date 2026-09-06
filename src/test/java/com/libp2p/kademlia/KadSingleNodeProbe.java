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

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class KadSingleNodeProbe {
    public static void main(String[] args) throws Exception {
        String file = System.getProperty("kad.extra", "");
        List<Multiaddr> seeds = new ArrayList<>();
        if (!file.isEmpty() && Files.exists(Path.of(file))) {
            for (String l : Files.readAllLines(Path.of(file))) {
                l = l.trim();
                if (!l.isEmpty()) { try { seeds.add(Multiaddr.fromString(l)); } catch (Exception ignore) {} }
            }
        }
        System.out.println("seeds: " + seeds.size());

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

        KadDht dht = new KadDht(KadConfig.builder().mode(KadMode.AUTO_SERVER)
                .bootstrapNodes(seeds)
                .queryTimeout(Duration.ofSeconds(60)).substreamTimeout(Duration.ofSeconds(20)).build());
        dht.setHost(host);
        dht.start().get(15, TimeUnit.SECONDS);

        List<Multiaddr> connected = new ArrayList<>();
        for (Multiaddr m : seeds) {
            try {
                io.libp2p.core.PeerId pid = m.getPeerId();
                host.getNetwork().connect(pid, m).get(25, TimeUnit.SECONDS);
                connected.add(m);
                System.out.println("connected: " + m);
            } catch (Exception e) {
                System.out.println("connect fail: " + m + " : " + e.getClass().getSimpleName());
            }
        }
        Thread.sleep(3000);
        System.out.flush();

        byte[] keyBytes = new byte[32];
        new SecureRandom().nextBytes(keyBytes);

        int ok = 0, fail = 0, timeout = 0;
        for (Multiaddr m : connected) {
            io.libp2p.core.PeerId pid = m.getPeerId();
            try {
                com.libp2p.kademlia.protocol.KademliaProtocol.FindNodeResponse resp = dht.getProtocol()
                        .sendFindNode(keyBytes, pid).get(25, TimeUnit.SECONDS);
                int closer = resp.closerPeers().size();
                if (closer > 0) {
                    ok++;
                    System.out.println("OK  " + m + " closer=" + closer);
                } else {
                    fail++;
                    System.out.println("EMPTY " + m + " closer=" + closer);
                }
            } catch (java.util.concurrent.TimeoutException te) {
                timeout++;
                System.out.println("TIMEOUT " + m);
            } catch (Exception e) {
                fail++;
                System.out.println("FAIL " + m + " : " + e.getClass().getSimpleName());
            }
        }
        System.out.println("SUMMARY ok=" + ok + " fail=" + fail + " timeout=" + timeout + " of " + connected.size());
        dht.close();
        host.stop().join();
        System.exit(0);
    }
}