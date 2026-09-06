package com.libp2p.kademlia.refresh;

import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HttpBootstrapPeerSourceTest {

    private static final String SAMPLE =
            "{\"Peers\":["
            + "{\"Addrs\":[\"/ip4/211.176.199.141/tcp/33363\"],\"ID\":\"QmX4PpsB4kn4s4muVXnykq7TtjPWkdaP6aAr3QUhEzZukZ\",\"Schema\":\"peer\"},"
            + "{\"Addrs\":[\"/ip4/45.77.219.216/tcp/4001\",\"/ip4/45.77.219.216/udp/4001/quic\",\"/ip4/45.77.219.216/udp/4001/quic-v1\",\"/ip4/45.77.219.216/udp/4001/quic-v1/webtransport/certhash/uEiX\"],\"ID\":\"12D3KooWHQnFuULtcGg9axasidmqVg8HHRw2XzKHPP8LjJpiwwHn\",\"Schema\":\"peer\"},"
            + "{\"Addrs\":[\"/dnsaddr/example.com\"],\"ID\":\"12D3KooWDead\",\"Schema\":\"peer\"}"
            + "]}";

    @Test
    void parsePeersKeepsAllTransports() {
        List<Multiaddr> peers = HttpBootstrapPeerSource.parsePeers(SAMPLE);
        assertEquals(5, peers.size(), "got: " + peers.stream().map(Multiaddr::toString).toList());
        assertTrue(peers.stream().anyMatch(m -> m.toString().equals("/ip4/211.176.199.141/tcp/33363/p2p/QmX4PpsB4kn4s4muVXnykq7TtjPWkdaP6aAr3QUhEzZukZ")));
        assertTrue(peers.stream().anyMatch(m ->
                m.toString().equals("/ip4/45.77.219.216/tcp/4001/p2p/12D3KooWHQnFuULtcGg9axasidmqVg8HHRw2XzKHPP8LjJpiwwHn")));
        assertTrue(peers.stream().anyMatch(m ->
                m.toString().equals("/ip4/45.77.219.216/udp/4001/quic/p2p/12D3KooWHQnFuULtcGg9axasidmqVg8HHRw2XzKHPP8LjJpiwwHn")));
        assertTrue(peers.stream().anyMatch(m ->
                m.toString().equals("/ip4/45.77.219.216/udp/4001/quic-v1/p2p/12D3KooWHQnFuULtcGg9axasidmqVg8HHRw2XzKHPP8LjJpiwwHn")));
        assertTrue(peers.stream().anyMatch(m -> m.toString().contains("/webtransport/")
                && m.toString().endsWith("/p2p/12D3KooWHQnFuULtcGg9axasidmqVg8HHRw2XzKHPP8LjJpiwwHn")));
        // dnsaddr is dropped here only because jvm-libp2p's multiaddr parser has no
        // dnsaddr protocol; every ip-based transport survives
        assertTrue(peers.stream().noneMatch(m -> m.toString().contains("dnsaddr")));
    }

    @Test
    void addrFiltersRestrictTransportsOnDemand() {
        java.util.function.Predicate<String> all = HttpBootstrapPeerSource.defaultAddrFilter();
        assertTrue(all.test("/ip4/1.2.3.4/udp/4001/quic-v1"));
        assertTrue(all.test("/ip4/1.2.3.4/udp/4001/webrtc-direct/certhash/uEiX"));
        assertTrue(all.test("/dnsaddr/example.com"));
        java.util.function.Predicate<String> tcpQuic = HttpBootstrapPeerSource.ipTcpQuicAddrFilter();
        assertTrue(tcpQuic.test("/ip4/1.2.3.4/tcp/4001"));
        assertTrue(tcpQuic.test("/ip4/1.2.3.4/udp/4001/quic-v1"));
        assertFalse(tcpQuic.test("/dnsaddr/example.com/tcp/4001"));
        assertFalse(tcpQuic.test("/ip4/1.2.3.4/udp/4001/webrtc-direct/certhash/uEiX"));
        assertFalse(tcpQuic.test("/ip4/1.2.3.4/udp/4001/quic-v1/webtransport/certhash/uEiX"));
    }

    @Test
    void parsePeersToleratesGarbage() {
        assertTrue(HttpBootstrapPeerSource.parsePeers("").isEmpty());
        assertTrue(HttpBootstrapPeerSource.parsePeers("{\"Peers\":[]}").isEmpty());
        assertTrue(HttpBootstrapPeerSource.parsePeers(null).isEmpty());
        assertTrue(HttpBootstrapPeerSource.parsePeers("{\"Peers\":[{\"Foo\":\"bar\",\"ID\":\"no-braces\"}").isEmpty());
    }

    @Test
    void buildKeysContainsSelfPeerIdAndRandomKeys() {
        PeerId self = PeerId.fromBase58("12D3KooWKnDdG3iXw9eTFijk3EWSunZcFi54Zka4wmtqtt6rPxc8");
        List<String> keys = HttpBootstrapPeerSource.buildKeys(self, 3);
        assertEquals(3, keys.size());
        assertTrue(keys.contains(self.toBase58()));
        for (String k : keys) {
            assertTrue(k.length() >= 46, "key too short: " + k);
        }
    }

    @Test
    void base32LowerMatchesReference() {
        assertEquals("", HttpBootstrapPeerSource.base32Lower(new byte[0]));
        assertEquals("MFRGG", HttpBootstrapPeerSource.base32Lower("abc".getBytes(StandardCharsets.US_ASCII)).toUpperCase());
    }

    @Test
    void generatedCidHasExpectedPrefix() {
        assertTrue(HttpBootstrapPeerSource.newRandomCidKey().startsWith("bafk"), HttpBootstrapPeerSource.newRandomCidKey());
    }

    @Test
    void defaultsAreSane() {
        assertTrue(HttpBootstrapPeerSource.defaults().getLookupKeys() >= 1);
    }

    @Test
    void defaultRoutersListDelegateAndFallback() {
        assertEquals(2, HttpBootstrapPeerSource.DEFAULT_ROUTERS.size());
        assertTrue(HttpBootstrapPeerSource.DEFAULT_ROUTERS.contains("https://delegated-ipfs.dev/routing/v1"));
        assertTrue(HttpBootstrapPeerSource.DEFAULT_ROUTERS.contains("https://cid.contact/routing/v1"));
        assertTrue(HttpBootstrapPeerSource.DEFAULT_ROUTERS.contains(HttpBootstrapPeerSource.DEFAULT_ROUTER));
        assertEquals(HttpBootstrapPeerSource.DEFAULT_ROUTERS.get(0), HttpBootstrapPeerSource.DEFAULT_ROUTER,
                "primary router must stay first/unweighted order");
    }

    @Test
    void dhtKeyIsDeterministicCidv1RawSha256() {
        byte[] key = new byte[32];
        for (int i = 0; i < key.length; i++) key[i] = (byte) i;
        String cid = HttpBootstrapPeerSource.dhtKeyFor(key);
        assertEquals(HttpBootstrapPeerSource.dhtKeyFor(key), cid, "same content key must map to the same routing key");
        assertNotEquals(HttpBootstrapPeerSource.dhtKeyFor(new byte[32]), cid);
        assertTrue(cid.startsWith("bafk"), "raw codec 0x55 sha256 cidv1 must start with bafk: " + cid);
        assertEquals(59, cid.length(), "cidv1 raw sha2-256 is 59 base32 chars: " + cid);
    }

    @Test
    void splitStringListHonorsEscapedQuotes() {
        assertEquals(List.of("/a", "/b"), HttpBootstrapPeerSource.splitStringList(" \"/a\" , \"/b\" "));
        assertEquals(List.of("a\"b", "c"), HttpBootstrapPeerSource.splitStringList("\"a\\\"b\",\"c\""));
        assertEquals(List.of(), HttpBootstrapPeerSource.splitStringList(""));
    }
}