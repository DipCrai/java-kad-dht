package com.libp2p.kademlia.routing;

import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class KadPeer {

    public final PeerId nodeId;
    public final List<Multiaddr> multiaddrs;
    public final ConnectionType connectionType;

    public KadPeer(PeerId nodeId, List<Multiaddr> multiaddrs, ConnectionType connectionType) {
        this.nodeId = nodeId;
        this.multiaddrs = new ArrayList<>(multiaddrs);
        this.connectionType = connectionType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KadPeer kadPeer = (KadPeer) o;
        return Objects.equals(nodeId, kadPeer.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(nodeId);
    }

    public byte[] toBytes() {
        return nodeId.getBytes();
    }

    @Override
    public String toString() {
        return "KadPeer{nodeId=" + nodeId + ", addrs.size=" + multiaddrs.size() +
                ", connectionType=" + connectionType + '}';
    }

    public enum ConnectionType {
        NOT_CONNECTED(0),
        CONNECTED(1),
        CAN_CONNECT(2),
        CANNOT_CONNECT(3);

        private final int value;

        ConnectionType(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static ConnectionType fromValue(int value) {
            for (ConnectionType type : values()) {
                if (type.value == value) {
                    return type;
                }
            }
            return NOT_CONNECTED;
        }
    }
}
