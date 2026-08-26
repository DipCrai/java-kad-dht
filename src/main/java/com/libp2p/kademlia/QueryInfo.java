package com.libp2p.kademlia;

import io.libp2p.core.multiformats.Multiaddr;

import java.util.List;

/**
 * Query-specific state.
 * Port of rust QueryInfo enum.
 */
public class QueryInfo {

    public enum Type {
        BOOTSTRAP,
        GET_CLOSEST_PEERS,
        FIND_NODE,
        GET_VALUE,
        PUT_RECORD,
        GET_PROVIDERS,
        ADD_PROVIDER,
        FIND_PEER
    }

    public final Type type;
    public final byte[] target;

    // Bootstrap-specific
    public int bootstrapPhase;
    public List<Integer> refreshableBuckets;

    // GetValue-specific
    public Record foundRecord;

    // PutRecord-specific
    public Record recordToPut;
    public int quorum;
    public int successes;

    // GetProviders-specific
    public java.util.function.Consumer<ProviderRecord> onProviderFound;

    // AddProvider-specific
    public ProviderRecord providerRecord;

    // FindPeer-specific
    public io.libp2p.core.PeerId targetPeer;

    private QueryInfo(Type type, byte[] target) {
        this.type = type;
        this.target = target;
    }

    public static QueryInfo bootstrap(byte[] selfKey) {
        return new QueryInfo(Type.BOOTSTRAP, selfKey);
    }

    public static QueryInfo getClosestPeers(byte[] target) {
        return new QueryInfo(Type.GET_CLOSEST_PEERS, target);
    }

    public static QueryInfo findNode(byte[] target) {
        return new QueryInfo(Type.FIND_NODE, target);
    }

    public static QueryInfo getValue(byte[] key) {
        return new QueryInfo(Type.GET_VALUE, key);
    }

    public static QueryInfo putRecord(Record record, int quorum) {
        QueryInfo info = new QueryInfo(Type.PUT_RECORD, record.getKey());
        info.recordToPut = record;
        info.quorum = quorum;
        return info;
    }

    public static QueryInfo getProviders(byte[] key) {
        return new QueryInfo(Type.GET_PROVIDERS, key);
    }

    public static QueryInfo addProvider(ProviderRecord record) {
        QueryInfo info = new QueryInfo(Type.ADD_PROVIDER, record.getKey());
        info.providerRecord = record;
        return info;
    }

    public static QueryInfo findPeer(io.libp2p.core.PeerId peerId, byte[] key) {
        QueryInfo info = new QueryInfo(Type.FIND_PEER, key);
        info.targetPeer = peerId;
        return info;
    }
}
