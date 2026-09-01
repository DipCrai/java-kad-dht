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
    void putValueGo() throws Exception {
        String addr = resolveNode("interop.go-passthru");
        Assumptions.assumeTrue(addr != null, "set -Dinterop.go-passthru=... to run");
        Multiaddr ma = Multiaddr.fromString(addr);
        PeerId pid = ma.getPeerId();
        KadDht dht = new KadDht(KadConfig.builder().mode(KadMode.CLIENT)
                .protocolName("/interop/kad/1.0.0")
                .queryTimeout(Duration.ofSeconds(10)).writeQuorum(1).readQuorum(1).build());
        Host h = newHost(dht);
        try {
            register(ma, h, dht);
            h.getAddressBook().addAddrs(h.getPeerId(), 60_000L, h.listenAddresses().get(0)).join();

            byte[] key = "/custom/putvalue-test-key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "hello-from-java".getBytes(StandardCharsets.UTF_8);

            com.libp2p.kademlia.records.WireRecord wireRec = new com.libp2p.kademlia.records.WireRecord(key, value);
            com.google.protobuf.ByteString bsKey = com.google.protobuf.ByteString.copyFrom(key);
            com.google.protobuf.ByteString bsValue = com.google.protobuf.ByteString.copyFrom(value);
            com.libp2p.kademlia.pb.Dht.Record rec = com.libp2p.kademlia.pb.Dht.Record.newBuilder()
                    .setKey(bsKey).setValue(bsValue).build();
            com.libp2p.kademlia.pb.Dht.Message putMsg = com.libp2p.kademlia.pb.Dht.Message.newBuilder()
                    .setType(com.libp2p.kademlia.pb.Dht.Message.MessageType.PUT_VALUE)
                    .setKey(bsKey).setRecord(rec).setClusterLevelRaw(10).build();
            boolean sent = dht.getProtocol().sendMessageFireAndForget(pid, putMsg).get(10, TimeUnit.SECONDS);
            System.out.println("### GO PUT_VALUE sent=" + sent);
            assertTrue(sent, "fire-and-forget PUT_VALUE should succeed");

            Thread.sleep(2000);

            var resp = dht.getProtocol().sendMessage(pid,
                    com.libp2p.kademlia.protocol.RpcCodec.getValue(key)).get(10, TimeUnit.SECONDS);
            System.out.println("### GO GET_VALUE hasRecord=" + resp.hasRecord());
            assertTrue(resp.hasRecord(), "getValue should return a record");
            assertArrayEquals(value, resp.getRecord().getValue().toByteArray());
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
            assertTrue(added, "Rust ADD_PROVIDER should succeed");

            Thread.sleep(1000);

            var resp = dht.getProtocol().sendMessage(pid,
                    com.libp2p.kademlia.protocol.RpcCodec.getProviders(key)).get(10, TimeUnit.SECONDS);
            System.out.println("### RUST GET_PROVIDERS providers=" + resp.getProviderPeersCount());
            assertTrue(resp.getProviderPeersCount() > 0, "Rust GET_PROVIDERS should return providers");
        } finally { dht.close(); h.stop().join(); }
    }
}
