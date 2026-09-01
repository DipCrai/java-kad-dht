package com.libp2p.kademlia.config;

/**
 * DHT operating mode.
 *
 * <ul>
 *   <li>{@link #SERVER} — accepts inbound queries, advertises via Identify, enters routing tables</li>
 *   <li>{@link #CLIENT} — can only send queries, does not advertise, not added to routing tables</li>
 *   <li>{@link #AUTO} — starts as CLIENT, upgrades to SERVER when external address confirmed</li>
 *   <li>{@link #AUTO_SERVER} — starts as SERVER immediately</li>
 * </ul>
 */
public enum KadMode {
    CLIENT, SERVER, AUTO, AUTO_SERVER;

    /** Whether this mode acts as a DHT server (accepts inbound queries). */
    public boolean isServer() {
        return this == SERVER || this == AUTO_SERVER;
    }

    /** Whether this mode is client-only (no inbound queries). */
    public boolean isClient() {
        return this == CLIENT;
    }

    /** Whether this mode supports automatic server upgrade. */
    public boolean isAuto() {
        return this == AUTO || this == AUTO_SERVER;
    }
}
