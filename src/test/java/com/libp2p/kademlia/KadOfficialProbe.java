package com.libp2p.kademlia;

import com.libp2p.kademlia.config.KadConfig;
import com.libp2p.kademlia.config.KadMode;
import com.libp2p.kademlia.protocol.KademliaProtocol;
import io.libp2p.core.Host;
import io.libp2p.core.crypto.KeyKt;
import io.libp2p.core.crypto.KeyType;
import io.libp2p.core.crypto.PrivKey;
import io.libp2p.core.dsl.Builder;
import io.libp2p.core.dsl.BuilderJKt;
import io.libp2p.core.mux.StreamMuxerProtocol;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.core.PeerId;
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

public class KadOfficialProbe {
    public static void main(String[] args) throws Exception {
        String file = System.getProperty("kad.extra", "");
        List<Multiaddr> seeds = new ArrayList<>();
        for (String l : Files.readAllLines(Path.of(file))) {
            l = l.trim();
            if (!l.isEmpty()) { try { seeds.add(Multiaddr.fromString(l)); } catch (Exception ignore) {} }
        }
        if (seeds.size() > 2) seeds = seeds.subList(0, 2);
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
                .queryTimeout(Duration.ofSeconds(75)).substreamTimeout(Duration.ofSeconds(20)).build());
        dht.setHost(host);
        dht.start().get(15, TimeUnit.SECONDS);

        byte[] k = new byte[32]; new SecureRandom().nextBytes(k);
        long t0 = System.currentTimeMillis();

        for (Multiaddr m : seeds) {
            PeerId pid = m.getPeerId();
            System.out.println("=== " + m);
            try {
                host.getNetwork().connect(pid, m).get(20, TimeUnit.SECONDS);
                System.out.println("  connected ok");
            } catch (Exception e) { System.out.println("  connect FAIL: " + chain(e)); continue; }
            try { Thread.sleep(2000); } catch (Exception ignore) {}
            // FIND_NODE, twice (first+retry pattern)
            for (int t = 0; t < 2; t++) {
                try {
                    KademliaProtocol.FindNodeResponse r = dht.getProtocol().sendFindNode(k, pid).get(15, TimeUnit.SECONDS);
                    System.out.println("  FIND_NODE try" + t + " OK closer=" + r.closerPeers().size());
                } catch (Exception e) { System.out.println("  FIND_NODE try" + t + " FAIL: " + chain(e)); }
            }
            // GET_PROVIDERS
            try {
                KademliaProtocol.GetProvidersResponse gp = dht.getProtocol().sendGetProviders("libp2p/relay".getBytes(), pid).get(15, TimeUnit.SECONDS);
                System.out.println("  GET_PROVIDERS OK provs=" + gp.providers().size() + " closer=" + gp.closerPeers().size());
            } catch (Exception e) { System.out.println("  GET_PROVIDERS FAIL: " + chain(e)); }
        }
        System.out.println("elapsed=" + (System.currentTimeMillis() - t0) + "ms");
        System.exit(0);
    }

    static String chain(Throwable e) {
        StringBuilder sb = new StringBuilder();
        Throwable cur = e;
        int i = 0;
        while (cur != null && i < 6) {
            sb.append(i++ == 0 ? "" : " <- ").append(cur.getClass().getSimpleName());
            String msg = String.valueOf(cur.getMessage());
            if (msg != null && msg.length() > 0 && !"null".equals(msg)) sb.append("[").append(msg.length() > 90 ? msg.substring(0, 90) : msg).append("]");
            cur = cur.getCause();
        }
        return sb.toString();
    }
}