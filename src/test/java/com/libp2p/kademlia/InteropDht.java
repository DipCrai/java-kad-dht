package com.libp2p.kademlia;

import com.libp2p.kademlia.config.KadConfig;
import com.libp2p.kademlia.config.KadMode;
import com.libp2p.kademlia.routing.KadPeer;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.crypto.KeyType;
import io.libp2p.core.crypto.KeyKt;
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
                .secureChannel(io.libp2p.security.noise.NoiseXXSecureChannel::new)
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

            var keyPair = KeyKt.generateKeyPair(KeyType.ED25519);
            io.libp2p.core.crypto.PubKey pubKey = keyPair.getSecond();
            byte[] pubKeyBytes = KeyKt.marshalPublicKey(pubKey);
            PeerId keyPeerId = PeerId.fromPubKey(pubKey);
            byte[] peerIdBytes = keyPeerId.getBytes();
            byte[] key = new byte[4 + peerIdBytes.length];
            System.arraycopy("/pk/".getBytes(StandardCharsets.UTF_8), 0, key, 0, 4);
            System.arraycopy(peerIdBytes, 0, key, 4, peerIdBytes.length);

            com.google.protobuf.ByteString bsKey = com.google.protobuf.ByteString.copyFrom(key);
            com.google.protobuf.ByteString bsValue = com.google.protobuf.ByteString.copyFrom(pubKeyBytes);
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
            assertArrayEquals(pubKeyBytes, resp.getRecord().getValue().toByteArray());
        } finally { dht.close(); h.stop().join(); }
    }

    @Test
    void putValueRust() throws Exception {
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
            byte[] k = "rust-put-test".getBytes(StandardCharsets.UTF_8);
            System.arraycopy(k, 0, key, 0, k.length);
            byte[] value = "hello-rust-value".getBytes(StandardCharsets.UTF_8);

            com.google.protobuf.ByteString bsKey = com.google.protobuf.ByteString.copyFrom(key);
            com.google.protobuf.ByteString bsValue = com.google.protobuf.ByteString.copyFrom(value);
            com.libp2p.kademlia.pb.Dht.Record rec = com.libp2p.kademlia.pb.Dht.Record.newBuilder()
                    .setKey(bsKey).setValue(bsValue).build();
            com.libp2p.kademlia.pb.Dht.Message putMsg = com.libp2p.kademlia.pb.Dht.Message.newBuilder()
                    .setType(com.libp2p.kademlia.pb.Dht.Message.MessageType.PUT_VALUE)
                    .setKey(bsKey).setRecord(rec).setClusterLevelRaw(10).build();
            boolean sent = dht.getProtocol().sendMessageFireAndForget(pid, putMsg).get(10, TimeUnit.SECONDS);
            System.out.println("### RUST PUT_VALUE sent=" + sent);
            assertTrue(sent, "fire-and-forget PUT_VALUE should succeed");

            Thread.sleep(2000);

            var resp = dht.getProtocol().sendMessage(pid,
                    com.libp2p.kademlia.protocol.RpcCodec.getValue(key)).get(10, TimeUnit.SECONDS);
            System.out.println("### RUST GET_VALUE hasRecord=" + resp.hasRecord());
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
        KadDht dht2 = new KadDht(KadConfig.builder().mode(KadMode.CLIENT)
                .queryTimeout(Duration.ofSeconds(10)).writeQuorum(1).readQuorum(1).build());
        Host h2 = newHost(dht2);
        try {
            register(ma, h, dht);
            register(ma, h2, dht2);
            h.getAddressBook().addAddrs(h.getPeerId(), 60_000L, h.listenAddresses().get(0)).join();
            h2.getAddressBook().addAddrs(h2.getPeerId(), 60_000L, h2.listenAddresses().get(0)).join();

            byte[] key = new byte[32];
            byte[] k = "rust-test-key".getBytes(StandardCharsets.UTF_8);
            System.arraycopy(k, 0, key, 0, k.length);

            boolean added = dht.getProtocol().sendAddProvider(key, pid).get(10, TimeUnit.SECONDS);
            System.out.println("### RUST ADD_PROVIDER=" + added);
            assertTrue(added, "Rust ADD_PROVIDER should succeed");

            Thread.sleep(1000);

            var resp = dht2.getProtocol().sendMessage(pid,
                    com.libp2p.kademlia.protocol.RpcCodec.getProviders(key)).get(10, TimeUnit.SECONDS);
            System.out.println("### RUST GET_PROVIDERS providers=" + resp.getProviderPeersCount());
            assertTrue(resp.getProviderPeersCount() > 0, "Rust GET_PROVIDERS should return providers");
        } finally { dht.close(); dht2.close(); h.stop().join(); h2.stop().join(); }
    }

    @Test
    void multiHopJavaGoJava() throws Exception {
        String addr = resolveNode("interop.go");
        Assumptions.assumeTrue(addr != null, "set -Dinterop.go=... to run");
        Multiaddr ma = Multiaddr.fromString(addr);
        PeerId pid = ma.getPeerId();

        KadDht dhtA = new KadDht(KadConfig.builder().mode(KadMode.CLIENT)
                .queryTimeout(Duration.ofSeconds(10)).writeQuorum(1).readQuorum(1).build());
        Host hA = newHost(dhtA);
        KadDht dhtB = new KadDht(KadConfig.builder().mode(KadMode.CLIENT)
                .queryTimeout(Duration.ofSeconds(10)).writeQuorum(1).readQuorum(1).build());
        Host hB = newHost(dhtB);
        try {
            register(ma, hA, dhtA);
            register(ma, hB, dhtB);

            var keyPair = KeyKt.generateKeyPair(KeyType.ED25519);
            io.libp2p.core.crypto.PubKey pubKey = keyPair.getSecond();
            byte[] pubKeyBytes = KeyKt.marshalPublicKey(pubKey);
            PeerId keyPeerId = PeerId.fromPubKey(pubKey);
            byte[] peerIdBytes = keyPeerId.getBytes();
            byte[] key = new byte[4 + peerIdBytes.length];
            System.arraycopy("/pk/".getBytes(StandardCharsets.UTF_8), 0, key, 0, 4);
            System.arraycopy(peerIdBytes, 0, key, 4, peerIdBytes.length);

            com.google.protobuf.ByteString bsKey = com.google.protobuf.ByteString.copyFrom(key);
            com.google.protobuf.ByteString bsValue = com.google.protobuf.ByteString.copyFrom(pubKeyBytes);
            com.libp2p.kademlia.pb.Dht.Record rec = com.libp2p.kademlia.pb.Dht.Record.newBuilder()
                    .setKey(bsKey).setValue(bsValue).build();
            com.libp2p.kademlia.pb.Dht.Message putMsg = com.libp2p.kademlia.pb.Dht.Message.newBuilder()
                    .setType(com.libp2p.kademlia.pb.Dht.Message.MessageType.PUT_VALUE)
                    .setKey(bsKey).setRecord(rec).setClusterLevelRaw(10).build();
            boolean sent = dhtA.getProtocol().sendMessageFireAndForget(pid, putMsg).get(10, TimeUnit.SECONDS);
            System.out.println("### MULTIHOP GO JAVA A→Go PUT=" + sent);
            assertTrue(sent);

            Thread.sleep(2000);

            var resp = dhtB.getProtocol().sendMessage(pid,
                    com.libp2p.kademlia.protocol.RpcCodec.getValue(key)).get(10, TimeUnit.SECONDS);
            System.out.println("### MULTIHOP GO JAVA B←Go GET=" + resp.hasRecord());
            assertTrue(resp.hasRecord(), "B should get record through Go");
            assertArrayEquals(pubKeyBytes, resp.getRecord().getValue().toByteArray());
        } finally { dhtA.close(); dhtB.close(); hA.stop().join(); hB.stop().join(); }
    }

    @Test
    void multiHopJavaRustJava() throws Exception {
        String addr = resolveNode("interop.rust");
        Assumptions.assumeTrue(addr != null, "set -Dinterop.rust=... to run");
        Multiaddr ma = Multiaddr.fromString(addr);
        PeerId pid = ma.getPeerId();

        KadDht dhtA = new KadDht(KadConfig.builder().mode(KadMode.CLIENT)
                .queryTimeout(Duration.ofSeconds(10)).writeQuorum(1).readQuorum(1).build());
        Host hA = newHost(dhtA);
        KadDht dhtB = new KadDht(KadConfig.builder().mode(KadMode.CLIENT)
                .queryTimeout(Duration.ofSeconds(10)).writeQuorum(1).readQuorum(1).build());
        Host hB = newHost(dhtB);
        try {
            register(ma, hA, dhtA);
            register(ma, hB, dhtB);

            byte[] key = new byte[32];
            byte[] k = "multihop-rust-java".getBytes(StandardCharsets.UTF_8);
            System.arraycopy(k, 0, key, 0, k.length);
            byte[] value = "via-rust-hop".getBytes(StandardCharsets.UTF_8);

            com.google.protobuf.ByteString bsKey = com.google.protobuf.ByteString.copyFrom(key);
            com.google.protobuf.ByteString bsValue = com.google.protobuf.ByteString.copyFrom(value);
            com.libp2p.kademlia.pb.Dht.Record rec = com.libp2p.kademlia.pb.Dht.Record.newBuilder()
                    .setKey(bsKey).setValue(bsValue).build();
            com.libp2p.kademlia.pb.Dht.Message putMsg = com.libp2p.kademlia.pb.Dht.Message.newBuilder()
                    .setType(com.libp2p.kademlia.pb.Dht.Message.MessageType.PUT_VALUE)
                    .setKey(bsKey).setRecord(rec).setClusterLevelRaw(10).build();
            boolean sent = dhtA.getProtocol().sendMessageFireAndForget(pid, putMsg).get(10, TimeUnit.SECONDS);
            System.out.println("### MULTIHOP RUST JAVA A→Rust PUT=" + sent);
            assertTrue(sent);

            Thread.sleep(2000);

            var resp = dhtB.getProtocol().sendMessage(pid,
                    com.libp2p.kademlia.protocol.RpcCodec.getValue(key)).get(10, TimeUnit.SECONDS);
            System.out.println("### MULTIHOP RUST JAVA B←Rust GET=" + resp.hasRecord());
            assertTrue(resp.hasRecord(), "B should get record through Rust");
            assertArrayEquals(value, resp.getRecord().getValue().toByteArray());
        } finally { dhtA.close(); dhtB.close(); hA.stop().join(); hB.stop().join(); }
    }

}
