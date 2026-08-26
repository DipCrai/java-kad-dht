package com.libp2p.kademlia;

import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.StreamPromise;
import io.libp2p.core.multiformats.Multiaddr;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class QueryEngine {
    private final QueryPool pool = new QueryPool();
    private final RoutingTable routingTable;
    private final KademliaConfig config;
    private volatile Host host;

    public QueryEngine(RoutingTable routingTable, KademliaConfig config) {
        this.routingTable = routingTable;
        this.config = config;
    }

    public void setHost(Host host) {
        this.host = host;
    }

    public CompletableFuture<List<KadPeer>> findClosestPeers(byte[] target) {
        List<KadPeer> seed = routingTable.findClosest(target, config.kValue);
        ClosestPeersIter iter = new ClosestPeersIter(target, seed, config.kValue, config.alphaValue, config.betaValue, config.substreamTimeout);
        Query query = new Query(QueryInfo.getClosestPeers(target), iter, config.queryTimeout);
        pool.addQuery(query);
        return query.getFuture();
    }

    public CompletableFuture<List<KadPeer>> findNode(byte[] target) {
        List<KadPeer> seed = routingTable.findClosest(target, config.kValue);
        ClosestPeersIter iter = new ClosestPeersIter(target, seed, config.kValue, config.alphaValue, config.betaValue, config.substreamTimeout);
        Query query = new Query(QueryInfo.findNode(target), iter, config.queryTimeout);
        pool.addQuery(query);
        return query.getFuture();
    }

    public CompletableFuture<Record> getValue(byte[] key) {
        List<KadPeer> seed = routingTable.findClosest(key, config.kValue);
        ClosestPeersIter iter = new ClosestPeersIter(key, seed, config.kValue, config.alphaValue, config.betaValue, config.substreamTimeout);
        Query query = new Query(QueryInfo.getValue(key), iter, config.queryTimeout);
        pool.addQuery(query);
        return query.getFuture().thenApply(peers -> query.info.foundRecord);
    }

    public CompletableFuture<List<KadPeer>> putRecord(Record record) {
        List<KadPeer> seed = routingTable.findClosest(record.getKey(), config.kValue);
        int quorum = Math.max(1, config.kValue / 2);
        ClosestPeersIter iter = new ClosestPeersIter(record.getKey(), seed, config.kValue, config.alphaValue, config.betaValue, config.substreamTimeout);
        Query query = new Query(QueryInfo.putRecord(record, quorum), iter, config.queryTimeout);
        pool.addQuery(query);
        return query.getFuture();
    }

    public CompletableFuture<List<KadPeer>> getProviders(byte[] key) {
        List<KadPeer> seed = routingTable.findClosest(key, config.kValue);
        ClosestPeersIter iter = new ClosestPeersIter(key, seed, config.kValue, config.alphaValue, config.betaValue, config.substreamTimeout);
        Query query = new Query(QueryInfo.getProviders(key), iter, config.queryTimeout);
        pool.addQuery(query);
        return query.getFuture();
    }

    public CompletableFuture<List<KadPeer>> addProvider(ProviderRecord record) {
        List<KadPeer> seed = routingTable.findClosest(record.getKey(), config.kValue);
        ClosestPeersIter iter = new ClosestPeersIter(record.getKey(), seed, config.kValue, config.alphaValue, config.betaValue, config.substreamTimeout);
        Query query = new Query(QueryInfo.addProvider(record), iter, config.queryTimeout);
        pool.addQuery(query);
        return query.getFuture();
    }

    public CompletableFuture<List<KadPeer>> bootstrap() {
        byte[] selfKey = XorId.fromPeerId(host.getPeerId());
        List<KadPeer> seed = routingTable.findClosest(selfKey, config.kValue);
        ClosestPeersIter iter = new ClosestPeersIter(selfKey, seed, config.kValue, config.alphaValue, config.betaValue, config.substreamTimeout);
        Query query = new Query(QueryInfo.bootstrap(selfKey), iter, config.queryTimeout);
        pool.addQuery(query);
        return query.getFuture();
    }

    public CompletableFuture<List<QueryPool.QueryAction>> poll() {
        return CompletableFuture.supplyAsync(() -> {
            List<QueryPool.QueryAction> actions = pool.poll();
            List<CompletableFuture<Void>> rpcFutures = new ArrayList<>();

            for (QueryPool.QueryAction action : actions) {
                if (action.type == QueryPool.QueryAction.Type.WAITING && action.peer != null) {
                    rpcFutures.add(sendRpc(action.query, action.peer));
                }
            }

            try {
                CompletableFuture.allOf(rpcFutures.toArray(CompletableFuture[]::new))
                        .get(config.substreamTimeout.toSeconds() * 2, TimeUnit.SECONDS);
            } catch (Exception ignored) {}

            return pool.poll();
        });
    }

    private CompletableFuture<Void> sendRpc(Query query, PeerId peer) {
        return CompletableFuture.runAsync(() -> {
            long start = System.currentTimeMillis();
            try {
                List<Multiaddr> addrs = getAddressesForPeer(peer);

                if (addrs.isEmpty()) {
                    query.iterator.onFailure(peer);
                    query.stats.recordFailure();
                    return;
                }

                StreamPromise<KademliaProtocol.KademliaController> promise = host.newStream(
                        List.of(config.protocolName), peer);
                KademliaProtocol.KademliaController ctrl = promise.getController()
                        .get(config.substreamTimeout.toSeconds(), TimeUnit.SECONDS);

                if (ctrl == null) {
                    query.iterator.onFailure(peer);
                    query.stats.recordFailure();
                    return;
                }

                byte[] targetKey = query.info.target;
                com.libp2p.kademlia.pb.Dht.Message req = com.libp2p.kademlia.pb.Dht.Message.newBuilder()
                        .setType(com.libp2p.kademlia.pb.Dht.Message.MessageType.FIND_NODE)
                        .setKey(com.google.protobuf.ByteString.copyFrom(targetKey))
                        .setClusterLevelRaw(10)
                        .build();

                com.libp2p.kademlia.pb.Dht.Message resp = ctrl.sendRequest(req)
                        .get(config.substreamTimeout.toSeconds(), TimeUnit.SECONDS);

                long elapsed = System.currentTimeMillis() - start;
                query.stats.recordSuccess(elapsed);

                List<KadPeer> closerPeers = parseCloserPeers(resp);
                for (KadPeer p : closerPeers) {
                    routingTable.insert(p.nodeId, p.multiaddrs);
                }

                query.iterator.onResponse(peer, closerPeers);

                processQuerySpecificResponse(query, resp, peer);

            } catch (Exception e) {
                query.iterator.onFailure(peer);
                query.stats.recordFailure();
            }
        });
    }

    private void processQuerySpecificResponse(Query query, com.libp2p.kademlia.pb.Dht.Message resp, PeerId peer) {
        switch (query.info.type) {
            case GET_VALUE -> {
                if (resp.hasRecord()) {
                    com.libp2p.kademlia.pb.Dht.Record pbRec = resp.getRecord();
                    Record record = new Record(
                            pbRec.getKey().toByteArray(),
                            pbRec.getValue().toByteArray(),
                            pbRec.hasPublisher() ? pbRec.getPublisher().toByteArray() : null,
                            pbRec.hasTtl() && pbRec.getTtl() > 0
                                    ? java.time.Instant.now().plusSeconds(pbRec.getTtl())
                                    : null
                    );
                    if (query.info.foundRecord == null) {
                        query.info.foundRecord = record;
                    }
                }
            }
            case PUT_RECORD -> {
                if (resp.hasRecord()) {
                    query.info.successes++;
                    if (query.info.successes >= query.info.quorum) {
                        query.iterator.getPeers().getAll().stream()
                                .filter(p -> p.state == PeerState.SUCCEEDED)
                                .limit(query.info.quorum)
                                .forEach(p -> {});
                    }
                }
            }
            default -> {}
        }
    }

    private List<Multiaddr> getAddressesForPeer(PeerId peer) {
        try {
            return new ArrayList<>(host.getAddressBook().getAddrs(peer)
                    .get(2, TimeUnit.SECONDS));
        } catch (Exception e) {
            return List.of();
        }
    }

    private List<KadPeer> parseCloserPeers(com.libp2p.kademlia.pb.Dht.Message msg) {
        List<KadPeer> peers = new ArrayList<>();
        for (com.libp2p.kademlia.pb.Dht.Message.Peer p : msg.getCloserPeersList()) {
            try {
                PeerId nodeId = new PeerId(p.getId().toByteArray());
                List<Multiaddr> addrs = new ArrayList<>();
                for (com.google.protobuf.ByteString ab : p.getAddrsList()) {
                    try {
                        addrs.add(Multiaddr.deserialize(ab.toByteArray()));
                    } catch (Exception ignored) {}
                }
                peers.add(new KadPeer(nodeId, addrs, KadPeer.ConnectionType.fromValue(p.getConnection().getNumber())));
            } catch (Exception ignored) {}
        }
        return peers;
    }

    public QueryPool getPool() { return pool; }
}
