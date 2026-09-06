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
import java.util.*;
import java.util.concurrent.TimeUnit;

public class KadCrawlProbe {
    private static KadDht dht;
    private static Host host;

    public static void main(String[] args) throws Exception {
        String file = System.getProperty("kad.extra", "");
        List<Multiaddr> seeds = new ArrayList<>();
        if (!file.isEmpty() && Files.exists(Path.of(file))) {
            for (String l : Files.readAllLines(Path.of(file))) {
                l = l.trim();
                if (!l.isEmpty()) { try { Multiaddr m = Multiaddr.fromString(l); if (isCrawlable(m)) seeds.add(m); } catch (Exception ignore) {} }
            }
        }
        System.out.println("seed pool: " + seeds.size() + " frontier=" + new ArrayDeque<>(seeds).size());

        PrivKey key = KeyKt.generateKeyPair(KeyType.ED25519).getFirst();
        host = BuilderJKt.hostJ(Builder.Defaults.Standard, b -> {
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

        dht = new KadDht(KadConfig.builder().mode(KadMode.AUTO_SERVER)
                .bootstrapNodes(seeds)
                .queryTimeout(Duration.ofSeconds(40)).substreamTimeout(Duration.ofSeconds(15)).build());
        dht.setHost(host);
        dht.start().get(15, TimeUnit.SECONDS);

        byte[] keyA = new byte[32];
        byte[] keyB = new byte[32];
        Arrays.fill(keyB, (byte) 0xFF);

        Set<String> seen = new HashSet<>();
        Set<String> alive = new LinkedHashSet<>();
        Deque<Multiaddr> frontier = new ArrayDeque<>(seeds);
        int levels = Integer.parseInt(System.getProperty("kad.levels", "1"));
        int perLevel = Integer.parseInt(System.getProperty("kad.perlevel", "25"));

        int level = 0;
        while (!frontier.isEmpty() && level <= levels) {
            System.out.println("== level " + level + ", frontier=" + frontier.size() + " aliveTotal=" + alive.size() + " ==");
            List<Multiaddr> next = new ArrayList<>();
            int processed = 0;
            while (!frontier.isEmpty() && processed++ < perLevel) {
                Multiaddr m = frontier.poll();
                String uid = m.toString();
                if (!seen.add(uid)) continue;
                PeerId pid = m.getPeerId();
                int closer = 0;
                try {
                    host.getNetwork().connect(pid, m).get(15, TimeUnit.SECONDS);
                } catch (Exception e) { System.out.println("  no-conn " + m); continue; }
                try {
                    Thread.sleep(2000);
                    KademliaProtocol.FindNodeResponse r1 = dht.getProtocol().sendFindNode(keyA, pid).get(15, TimeUnit.SECONDS);
                    KademliaProtocol.FindNodeResponse r2 = dht.getProtocol().sendFindNode(keyB, pid).get(15, TimeUnit.SECONDS);
                    closer = r1.closerPeers().size() + r2.closerPeers().size();
                    for (var cp : r1.closerPeers()) addNext(cp, next, seen);
                    for (var cp : r2.closerPeers()) addNext(cp, next, seen);
                    alive.add(uid);
                    System.out.println("  ALIVE " + m + " closer=" + closer + " totalNew=" + next.size());
                } catch (Exception e) {
                    System.out.println("  bad   " + m);
                }
            }
            frontier.addAll(next);
            level++;
        }
        System.out.println("CRAWL DONE alive=" + alive.size() + " totalDiscovered=" + seen.size());
        try { Files.write(Path.of("/tmp/crawled_live.txt"), alive); } catch (Exception ignore) {}
        System.exit(0);
    }

    private static void addNext(com.libp2p.kademlia.routing.KadPeer cp, List<Multiaddr> next, Set<String> seen) {
        for (Multiaddr a : cp.multiaddrs) {
            if (!isCrawlable(a)) continue;
            String uid = a.toString();
            if (seen.add(uid)) next.add(a);
        }
    }

    private static boolean isCrawlable(Multiaddr m) {
        String s = m.toString();
        return s.contains("/tcp/") && (s.contains("/ip4/") || s.contains("/ip6/")) && s.contains("/p2p/");
    }
}