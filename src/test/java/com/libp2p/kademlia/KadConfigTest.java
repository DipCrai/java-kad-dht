package com.libp2p.kademlia;

import com.libp2p.kademlia.bootstrap.DefaultBootstrapPeers;
import com.libp2p.kademlia.config.KadConfig;
import com.libp2p.kademlia.config.KadMode;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class KadConfigTest {

    @Test
    void testDefaults() {
        KadConfig config = KadConfig.builder().build();
        assertEquals("/ipfs/kad/1.0.0", config.getProtocolName());
        assertEquals(20, config.getKValue());
        assertEquals(10, config.getAlphaValue());
        assertEquals(3, config.getBetaValue());
        assertEquals(Duration.ofSeconds(60), config.getQueryTimeout());
        assertEquals(Duration.ofSeconds(10), config.getSubstreamTimeout());
        assertEquals(16384, config.getMaxPacketSize());
        assertEquals(Duration.ofMinutes(5), config.getBootstrapInterval());
        assertEquals(Duration.ofSeconds(60), config.getPendingTimeout());
        assertEquals(Duration.ofHours(48), config.getProviderRecordTTL());
        assertEquals(Duration.ofMinutes(30), config.getProviderAddrTTL());
        assertEquals(Duration.ofHours(48), config.getRecordMaxAge());
        assertEquals(Duration.ofHours(1), config.getRecordReplicationInterval());
        assertEquals(Duration.ofHours(22), config.getRecordPublicationInterval());
        assertEquals(Duration.ofHours(12), config.getProviderPublicationInterval());
        assertEquals(1024, config.getMaxRecords());
        assertEquals(1024, config.getMaxProvidedKeys());
        assertEquals(20, config.getMaxProvidersPerKey());
        assertEquals(65536, config.getMaxRecordValueSize());
        assertEquals(100, config.getMaxConcurrentQueries());
        assertEquals(100, config.getMaxInboundRequests());
        assertEquals(20, config.getReplicationFactor());
        assertEquals(1, config.getWriteQuorum());
        assertEquals(1, config.getReadQuorum());
        assertEquals(1, config.getDisjointPaths());
        assertEquals(KadMode.AUTO_SERVER, config.getMode());
        assertEquals(DefaultBootstrapPeers.DEFAULT_BOOTSTRAP_PEERS.size(), config.getBootstrapNodes().size());
        assertTrue(config.getBootstrapNodes().containsAll(DefaultBootstrapPeers.DEFAULT_BOOTSTRAP_PEERS));
        assertEquals(Duration.ofMinutes(5), config.getBootstrapAddressTTL());
        assertEquals(Duration.ofMinutes(30), config.getPeerAddressTTL());
    }

    @Test
    void testBuilder() {
        KadConfig config = KadConfig.builder()
                .protocolName("/test/kad")
                .kValue(10)
                .alphaValue(5)
                .betaValue(4)
                .queryTimeout(Duration.ofSeconds(30))
                .substreamTimeout(Duration.ofSeconds(5))
                .maxPacketSize(8192)
                .bootstrapInterval(Duration.ofMinutes(10))
                .pendingTimeout(Duration.ofSeconds(30))
                .providerRecordTTL(Duration.ofHours(24))
                .providerAddrTTL(Duration.ofMinutes(15))
                .recordMaxAge(Duration.ofHours(24))
                .maxRecords(512)
                .maxProvidedKeys(512)
                .maxProvidersPerKey(10)
                .maxRecordValueSize(32768)
                .maxConcurrentQueries(50)
                .maxInboundRequests(50)
                .replicationFactor(10)
                .writeQuorum(3)
                .readQuorum(3)
                .disjointPaths(2)
                .mode(KadMode.SERVER)
                .build();
        assertEquals("/test/kad", config.getProtocolName());
        assertEquals(10, config.getKValue());
        assertEquals(5, config.getAlphaValue());
        assertEquals(4, config.getBetaValue());
        assertEquals(Duration.ofSeconds(30), config.getQueryTimeout());
        assertEquals(512, config.getMaxRecords());
        assertEquals(10, config.getMaxProvidersPerKey());
        assertEquals(10, config.getReplicationFactor());
        assertEquals(3, config.getWriteQuorum());
        assertEquals(3, config.getReadQuorum());
        assertEquals(2, config.getDisjointPaths());
        assertEquals(KadMode.SERVER, config.getMode());
    }

    @Test
    void testValidationRejectsAlphaZero() {
        assertThrows(IllegalArgumentException.class, () -> {
            KadConfig.builder().alphaValue(0).build();
        });
    }

    @Test
    void testValidationRejectsWriteQuorumAboveReplicationFactor() {
        assertThrows(IllegalArgumentException.class, () -> {
            KadConfig.builder().writeQuorum(25).replicationFactor(20).build();
        });
    }

    @Test
    void testValidationRejectsKZero() {
        assertThrows(IllegalArgumentException.class, () -> {
            KadConfig.builder().kValue(0).build();
        });
    }

    @Test
    void testModeIsServer() {
        assertTrue(KadMode.SERVER.isServer());
        assertTrue(KadMode.AUTO_SERVER.isServer());
        assertFalse(KadMode.CLIENT.isServer());
        assertFalse(KadMode.AUTO.isServer());
    }

    @Test
    void testModeIsClient() {
        assertTrue(KadMode.CLIENT.isClient());
        assertFalse(KadMode.SERVER.isClient());
        assertFalse(KadMode.AUTO.isClient());
        assertFalse(KadMode.AUTO_SERVER.isClient());
    }

    @Test
    void testBootstrapAddressTTLCustom() {
        KadConfig config = KadConfig.builder()
                .bootstrapAddressTTL(Duration.ofMinutes(15))
                .build();
        assertEquals(Duration.ofMinutes(15), config.getBootstrapAddressTTL());
    }

    @Test
    void testPeerAddressTTLCustom() {
        KadConfig config = KadConfig.builder()
                .peerAddressTTL(Duration.ofHours(2))
                .build();
        assertEquals(Duration.ofHours(2), config.getPeerAddressTTL());
    }

    @Test
    void testAddressTTLDefaults() {
        KadConfig config = KadConfig.builder().build();
        assertEquals(Duration.ofMinutes(5), config.getBootstrapAddressTTL());
        assertEquals(Duration.ofMinutes(30), config.getPeerAddressTTL());
    }

    @Test
    void testAddressTTLInBuilder() {
        KadConfig config = KadConfig.builder()
                .bootstrapAddressTTL(Duration.ofMinutes(10))
                .peerAddressTTL(Duration.ofMinutes(45))
                .build();
        assertEquals(Duration.ofMinutes(10), config.getBootstrapAddressTTL());
        assertEquals(Duration.ofMinutes(45), config.getPeerAddressTTL());
    }
}
