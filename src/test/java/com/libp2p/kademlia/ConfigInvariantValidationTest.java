package com.libp2p.kademlia;

import com.libp2p.kademlia.config.KadConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class ConfigInvariantValidationTest {

    private KadConfig.Builder base() {
        return KadConfig.builder();
    }

    @Test
    void testKValueMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> base().kValue(0).build());
        assertThrows(IllegalArgumentException.class, () -> base().kValue(-1).build());
    }

    @Test
    void testAlphaValueMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> base().alphaValue(0).build());
        assertThrows(IllegalArgumentException.class, () -> base().alphaValue(-5).build());
    }

    @Test
    void testAlphaValueMustNotExceedK() {
        assertThrows(IllegalArgumentException.class,
                () -> base().kValue(10).alphaValue(11).build());
    }

    @Test
    void testBetaValueMustNotBeNegative() {
        assertThrows(IllegalArgumentException.class, () -> base().betaValue(-1).build());
        assertThrows(IllegalArgumentException.class, () -> base().betaValue(-100).build());
    }

    @Test
    void testBetaValueMustNotExceedK() {
        assertThrows(IllegalArgumentException.class,
                () -> base().kValue(10).betaValue(11).build());
    }

    @Test
    void testNegativeQueryTimeoutRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> base().queryTimeout(Duration.ofMillis(-1)).build());
        assertThrows(IllegalArgumentException.class,
                () -> base().queryTimeout(Duration.ZERO).build());
        assertThrows(IllegalArgumentException.class,
                () -> base().queryTimeout(null).build());
    }

    @Test
    void testWriteQuorumMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> base().writeQuorum(0).build());
        assertThrows(IllegalArgumentException.class, () -> base().writeQuorum(-2).build());
    }

    @Test
    void testReadQuorumMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> base().readQuorum(0).build());
        assertThrows(IllegalArgumentException.class, () -> base().readQuorum(-1).build());
    }

    @Test
    void testDisjointPathsMustBeAtLeastOne() {
        assertThrows(IllegalArgumentException.class, () -> base().disjointPaths(0).build());
        assertThrows(IllegalArgumentException.class, () -> base().disjointPaths(-3).build());
    }

    @Test
    void testValidAtomicConfigBuilds() {
        KadConfig config = base()
                .kValue(20)
                .alphaValue(3)
                .betaValue(3)
                .disjointPaths(3)
                .build();
        assertEquals(20, config.getKValue());
        assertEquals(3, config.getAlphaValue());
        assertEquals(3, config.getBetaValue());
        assertEquals(3, config.getDisjointPaths());
    }
}
