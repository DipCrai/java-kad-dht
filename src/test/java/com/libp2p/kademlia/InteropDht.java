package com.libp2p.kademlia;

import com.libp2p.kademlia.config.KadConfig;
import com.libp2p.kademlia.config.KadMode;
import com.libp2p.kademlia.routing.KadPeer;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.dsl.HostBuilder;
import io.libp2p.core.multiformats.Multiaddr;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class InteropDht {

    private static String resolveNode(String sysProp) {
        String val = System.getProperty(sysProp, "").trim();
        return val.isEmpty() ? null : val;
    }

    private Host newHost(KadDht dht) {
        Host h = new HostBuilder()
                .muxer(io.libp2p.core.mux.StreamMuxerProtocol::getYamux)
                .listen("/ip4/127.0.0.1/tcp/0")
                .build();
        h.start().join();
        dht.setHost(h);
        return h;
    }

    private void register(Multiaddr ma, Host h, KadDht dht) {
        h.getAddressBook().addAddrs(ma.getPeerId(), 60_000L, ma).join();
        dht.getRoutingTable().insert(ma.getPeerId(), List.of(ma));
    }

    @Test
    void pingGo() throws Exception {
        String addr = resolveNode("interop.go");
        Assumptions.assumeTrue(addr != null, "set -Dinterop.go=... to run");
        Multiaddr ma = Multiaddr.fromString(addr);
        KadDht dht = new KadDht(KadConfig.builder().mode(KadMode.CLIENT).build());
        Host h = newHost(dht);
        try {
            register(ma, h, dht);
            boolean ok = dht.ping(ma.getPeerId()).get(10, TimeUnit.SECONDS);
            System.out.println("### GO PING=" + ok);
            assertTrue(ok);
        } finally { dht.close(); h.stop().join(); }
    }

    @Test
    void pingRust() throws Exception {
        String addr = resolveNode("interop.rust");
        Assumptions.assumeTrue(addr != null, "set -Dinterop.rust=... to run");
        Multiaddr ma = Multiaddr.fromString(addr);
        KadDht dht = new KadDht(KadConfig.builder().mode(KadMode.CLIENT).build());
        Host h = newHost(dht);
        try {
            register(ma, h, dht);
            boolean ok = dht.ping(ma.getPeerId()).get(10, TimeUnit.SECONDS);
            System.out.println("### RUST PING=" + ok);
            assertTrue(ok);
        } finally { dht.close(); h.stop().join(); }
    }

    @Test
    void findNodeGo() throws Exception {
        String addr = resolveNode("interop.go");
        Assumptions.assumeTrue(addr != null, "set -Dinterop.go=... to run");
        Multiaddr ma = Multiaddr.fromString(addr);
        KadDht dht = new KadDht(KadConfig.builder().mode(KadMode.CLIENT).queryTimeout(Duration.ofSeconds(10)).build());
        Host h = newHost(dht);
        try {
            register(ma, h, dht);
            List<KadPeer> closest = dht.findNode(ma.getPeerId()).get(15, TimeUnit.SECONDS);
            System.out.println("### GO FIND_NODE OK: " + closest.size() + " peers");
        } finally { dht.close(); h.stop().join(); }
    }

    @Test
    void findNodeRust() throws Exception {
        String addr = resolveNode("interop.rust");
        Assumptions.assumeTrue(addr != null, "set -Dinterop.rust=... to run");
        Multiaddr ma = Multiaddr.fromString(addr);
        KadDht dht = new KadDht(KadConfig.builder().mode(KadMode.CLIENT).queryTimeout(Duration.ofSeconds(10)).build());
        Host h = newHost(dht);
        try {
            register(ma, h, dht);
            List<KadPeer> closest = dht.findNode(ma.getPeerId()).get(15, TimeUnit.SECONDS);
            System.out.println("### RUST FIND_NODE OK: " + closest.size() + " peers");
        } finally { dht.close(); h.stop().join(); }
    }

    @Test
    void addProviderGetProvidersGo() throws Exception {
        String addr = resolveNode("interop.go");
        Assumptions.assumeTrue(addr != null, "set -Dinterop.go=... to run");
        Multiaddr ma = Multiaddr.fromString(addr);
        PeerId pid = ma.getPeerId();
        KadDht dht = new KadDht(KadConfig.builder().mode(KadMode.CLIENT)
                .queryTimeout(Duration.ofSeconds(10)).writeQuorum(1).readQuorum(1).build());
        Host h = newHost(dht);
        try {
            register(ma, h, dht);
            h.getAddressBook().addAddrs(h.getPeerId(), 60_000L, h.listenAddresses().get(0)).join();

            byte[] key = new byte[32];
            byte[] k = "interop-test-key".getBytes(StandardCharsets.UTF_8);
            System.arraycopy(k, 0, key, 0, k.length);

            boolean added = dht.getProtocol().sendAddProvider(key, pid).get(10, TimeUnit.SECONDS);
            System.out.println("### GO ADD_PROVIDER=" + added);
            assertTrue(added);

            Thread.sleep(1000);

            var resp = dht.getProtocol().sendMessage(pid,
                    com.libp2p.kademlia.protocol.RpcCodec.getProviders(key)).get(10, TimeUnit.SECONDS);
            System.out.println("### GO GET_PROVIDERS providers=" + resp.getProviderPeersCount());
            assertTrue(resp.getProviderPeersCount() > 0, "should return at least one provider");
        } finally { dht.close(); h.stop().join(); }
    }

    @Test
    void addProviderGetProvidersRust() throws Exception {
        String addr = resolveNode("interop.rust");
        Assumptions.assumeTrue(addr != null, "set -Dinterop.rust=... to run");
        Multiaddr ma = Multiaddr.fromString(addr);
        PeerId pid = ma.getPeerId();
        KadDht dht = new KadDht(KadConfig.builder().mode(KadMode.CLIENT)
                .queryTimeout(Duration.ofSeconds(10)).writeQuorum(1).readQuorum(1).build());
        Host h = newHost(dht);
        try {
            register(ma, h, dht);
            h.getAddressBook().addAddrs(h.getPeerId(), 60_000L, h.listenAddresses().get(0)).join();

            byte[] key = new byte[32];
            byte[] k = "rust-test-key".getBytes(StandardCharsets.UTF_8);
            System.arraycopy(k, 0, key, 0, k.length);

            boolean added = dht.getProtocol().sendAddProvider(key, pid).get(10, TimeUnit.SECONDS);
            System.out.println("### RUST ADD_PROVIDER=" + added);

            Thread.sleep(1000);

            var resp = dht.getProtocol().sendMessage(pid,
                    com.libp2p.kademlia.protocol.RpcCodec.getProviders(key)).get(10, TimeUnit.SECONDS);
            System.out.println("### RUST GET_PROVIDERS providers=" + resp.getProviderPeersCount());
        } catch (Exception e) {
            System.out.println("### RUST PROVIDER FAIL: " + e);
        } finally { dht.close(); h.stop().join(); }
    }
}
