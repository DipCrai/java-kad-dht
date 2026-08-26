package com.libp2p.kademlia.protocol;

import com.google.protobuf.ByteString;
import com.libp2p.kademlia.pb.Dht;
import com.libp2p.kademlia.records.WireRecord;

public final class RpcCodec {
    private RpcCodec() {}

    public static byte[] encode(Dht.Message msg) {
        byte[] payload = msg.toByteArray();
        byte[] varint = encodeVarint(payload.length);
        byte[] result = new byte[varint.length + payload.length];
        System.arraycopy(varint, 0, result, 0, varint.length);
        System.arraycopy(payload, 0, result, varint.length, payload.length);
        return result;
    }

    public static byte[] encodeVarint(int value) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        while ((value & ~0x7F) != 0) { out.write((value & 0x7F) | 0x80); value >>>= 7; }
        out.write(value);
        return out.toByteArray();
    }

    public static Dht.Message findNode(byte[] key) {
        return Dht.Message.newBuilder().setType(Dht.Message.MessageType.FIND_NODE)
                .setKey(ByteString.copyFrom(key)).setClusterLevelRaw(10).build();
    }

    public static Dht.Message ping() {
        return Dht.Message.newBuilder().setType(Dht.Message.MessageType.PING).build();
    }

    public static Dht.Message getValue(byte[] key) {
        return Dht.Message.newBuilder().setType(Dht.Message.MessageType.GET_VALUE)
                .setKey(ByteString.copyFrom(key)).setClusterLevelRaw(10).build();
    }

    public static Dht.Message putValue(WireRecord record) {
        Dht.Record pbRec = Dht.Record.newBuilder()
                .setKey(ByteString.copyFrom(record.getKey()))
                .setValue(ByteString.copyFrom(record.getValue()))
                .setTimeReceived(java.time.Instant.now().toString()).build();
        return Dht.Message.newBuilder().setType(Dht.Message.MessageType.PUT_VALUE)
                .setRecord(pbRec).setClusterLevelRaw(10).build();
    }

    public static Dht.Message addProvider(byte[] key, byte[] selfPeerId, java.util.List<io.libp2p.core.multiformats.Multiaddr> addrs) {
        Dht.Message.Peer.Builder peerBuilder = Dht.Message.Peer.newBuilder()
                .setId(ByteString.copyFrom(selfPeerId))
                .setConnection(Dht.Message.ConnectionType.CONNECTED);
        if (addrs != null) {
            for (io.libp2p.core.multiformats.Multiaddr addr : addrs) {
                peerBuilder.addAddrs(ByteString.copyFrom(addr.serialize()));
            }
        }
        return Dht.Message.newBuilder().setType(Dht.Message.MessageType.ADD_PROVIDER)
                .setKey(ByteString.copyFrom(key)).addProviderPeers(peerBuilder.build()).setClusterLevelRaw(10).build();
    }

    public static Dht.Message getProviders(byte[] key) {
        return Dht.Message.newBuilder().setType(Dht.Message.MessageType.GET_PROVIDERS)
                .setKey(ByteString.copyFrom(key)).setClusterLevelRaw(10).build();
    }

    public static Dht.Message pong() {
        return Dht.Message.newBuilder().setType(Dht.Message.MessageType.PING).build();
    }
}
