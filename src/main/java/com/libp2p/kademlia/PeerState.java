package com.libp2p.kademlia;

import java.time.Instant;

/**
 * Per-peer state within a query.
 * Port of rust query::peers::PeerState.
 */
public enum PeerState {
    /** Known but not yet contacted. */
    NOT_CONTACTED,
    /** Currently being queried. */
    WAITING,
    /** Timed out or unresponsive. */
    UNRESPONSIVE,
    /** Query failed with error. */
    FAILED,
    /** Successfully responded. */
    SUCCEEDED;

    public boolean isPending() {
        return this == NOT_CONTACTED || this == WAITING;
    }

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == UNRESPONSIVE;
    }
}
