package com.libp2p.kademlia.protocol;

import com.google.protobuf.ByteString;
import com.libp2p.kademlia.pb.Dht;
import com.libp2p.kademlia.records.Record;
import com.libp2p.kademlia.records.RecordStore;
import com.libp2p.kademlia.records.ProviderStore;
import com.libp2p.kademlia.records.ProviderRecord;
import com.libp2p.kademlia.routing.RoutingTable;
import com.libp2p.kademlia.routing.KadPeer;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.core.multistream.ProtocolBinding;
import io.libp2p.core.multistream.ProtocolDescriptor;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class KademliaProtocol implements ProtocolBinding<KademliaProtocol.KademliaController> {

    private final String protocolName;
    private final int kValue;
    private final Duration substreamTimeout;
    private final Duration providerRecordTTL;
    private final Duration providerAddrTTL;
    private volatile RoutingTable routingTable;
    private volatile Host host;
    private volatile RecordStore recordStore;
    private volatile ProviderStore providerStore;
    private volatile com.libp2p.kademlia.records.RecordValidator validator = com.libp2p.kademlia.records.RecordValidator.NOOP;
    private volatile boolean serverMode = true;

    public KademliaProtocol(String protocolName, int kValue, Duration substreamTimeout, Duration providerRecordTTL, Duration providerAddrTTL) {
        this.protocolName = protocolName;
        this.kValue = kValue;
        this.substreamTimeout = substreamTimeout;
        this.providerRecordTTL = providerRecordTTL;
        this.providerAddrTTL = providerAddrTTL;
    }

    @Override
    public ProtocolDescriptor getProtocolDescriptor() { return new ProtocolDescriptor(protocolName); }

    @Override
    public CompletableFuture<KademliaController> initChannel(P2PChannel ch, String selectedProtocol) {
        Stream stream = (Stream) ch;
        if (!stream.isInitiator()) {
            if (!serverMode) {
                stream.close();
                return CompletableFuture.failedFuture(new IllegalStateException("Client mode: not accepting inbound streams"));
            }
            ResponderHandler handler = new ResponderHandler(this, stream);
            stream.pushHandler(new KademliaCodec());
            stream.pushHandler(handler);
            return CompletableFuture.completedFuture(handler);
        } else {
            InitiatorHandler handler = new InitiatorHandler(stream);
            stream.pushHandler(new KademliaCodec());
            stream.pushHandler(handler);
            return CompletableFuture.completedFuture(handler);
        }
    }

    public void setHost(Host host) { this.host = host; }
    public void setRoutingTable(RoutingTable rt) { this.routingTable = rt; }
    public void setRecordStore(RecordStore store) { this.recordStore = store; }
    public void setProviderStore(ProviderStore store) { this.providerStore = store; }
    public void setValidator(com.libp2p.kademlia.records.RecordValidator v) { this.validator = v; }
    public void setServerMode(boolean serverMode) { this.serverMode = serverMode; }
    public boolean isServerMode() { return serverMode; }

    public CompletableFuture<Dht.Message> sendMessage(PeerId peer, Dht.Message msg) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                StreamPromise<KademliaController> promise = host.newStream(List.of(protocolName), peer);
                KademliaController ctrl = promise.getController().get(substreamTimeout.toSeconds(), TimeUnit.SECONDS);
                if (ctrl == null) throw new IllegalStateException("No controller");
                return ctrl.sendRequest(msg).get(substreamTimeout.toSeconds(), TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
    }

    public CompletableFuture<FindNodeResponse> sendFindNode(byte[] key, PeerId peer) {
        return sendMessage(peer, RpcCodec.findNode(key)).thenApply(msg -> FindNodeResponse.fromMessage(msg, providerRecordTTL, providerAddrTTL));
    }

    public CompletableFuture<GetValueResponse> sendGetValue(byte[] key, PeerId peer) {
        return sendMessage(peer, RpcCodec.getValue(key)).thenApply(msg -> GetValueResponse.fromMessage(msg, recordStore));
    }

    public CompletableFuture<Boolean> sendPutValue(Record record, PeerId peer) {
        return sendMessage(peer, RpcCodec.putValue(record)).thenApply(msg ->
                msg.hasRecord() && Arrays.equals(msg.getRecord().getKey().toByteArray(), record.getKey()));
    }

    public CompletableFuture<Boolean> pingLiveness(PeerId peer, Duration timeout) {
        if (host == null) return CompletableFuture.completedFuture(false);
        try {
            StreamPromise<io.libp2p.protocol.PingController> promise = host.newStream(List.of("/ipfs/ping/1.0.0"), peer);
            io.libp2p.protocol.PingController ctrl = promise.getController().get(timeout.toSeconds(), TimeUnit.SECONDS);
            if (ctrl == null) return CompletableFuture.completedFuture(false);
            return ctrl.ping().thenApply(pong -> true).exceptionally(ex -> false);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(false);
        }
    }

    public CompletableFuture<Boolean> sendAddProvider(byte[] key, PeerId peer) {
        return sendMessage(peer, RpcCodec.addProvider(key, host.getPeerId().getBytes())).thenApply(msg -> true);
    }

    public CompletableFuture<GetProvidersResponse> sendGetProviders(byte[] key, PeerId peer) {
        return sendMessage(peer, RpcCodec.getProviders(key)).thenApply(msg -> GetProvidersResponse.fromMessage(msg, providerStore, providerRecordTTL, providerAddrTTL));
    }

    Dht.Message handlePing() { return RpcCodec.pong(); }

    Dht.Message handleFindNode(Dht.Message req, PeerId requester) {
        byte[] key = req.getKey().toByteArray();
        List<KadPeer> closer = routingTable.findClosest(key, kValue);
        Dht.Message.Builder builder = Dht.Message.newBuilder().setType(Dht.Message.MessageType.FIND_NODE).setKey(ByteString.copyFrom(key));
        for (KadPeer p : closer) { if (!p.nodeId.equals(requester)) builder.addCloserPeers(toProtoPeer(p)); }
        return builder.build();
    }

    Dht.Message handleGetValue(Dht.Message req, PeerId requester) {
        byte[] key = req.getKey().toByteArray();
        Dht.Message.Builder builder = Dht.Message.newBuilder().setType(Dht.Message.MessageType.GET_VALUE).setKey(ByteString.copyFrom(key));
        if (recordStore != null) {
            Record record = recordStore.get(key);
            if (record != null) {
                Dht.Record.Builder rb = Dht.Record.newBuilder()
                        .setKey(ByteString.copyFrom(record.getKey()))
                        .setValue(ByteString.copyFrom(record.getValue()));
                if (record.getPublisher() != null) rb.setPublisher(ByteString.copyFrom(record.getPublisher()));
                if (record.getTimeReceived() != null) rb.setTimeReceived(record.getTimeReceived().toString());
                builder.setRecord(rb.build());
            }
        }
        for (KadPeer p : routingTable.findClosest(key, kValue)) { if (!p.nodeId.equals(requester)) builder.addCloserPeers(toProtoPeer(p)); }
        return builder.build();
    }

    Dht.Message handlePutValue(Dht.Message req, PeerId requester) {
        if (!req.hasRecord()) return Dht.Message.newBuilder().setType(Dht.Message.MessageType.PUT_VALUE).build();
        Dht.Record pbRec = req.getRecord();
        if (pbRec.getKey().isEmpty()) return Dht.Message.newBuilder().setType(Dht.Message.MessageType.PUT_VALUE).build();
        byte[] key = pbRec.getKey().toByteArray();
        byte[] value = pbRec.getValue().toByteArray();
        if (recordStore != null && value.length > 0) {
            if (!validator.validate(key, value)) return Dht.Message.newBuilder().setType(Dht.Message.MessageType.PUT_VALUE).build();
            byte[] publisher = pbRec.hasPublisher() && !pbRec.getPublisher().isEmpty()
                    ? pbRec.getPublisher().toByteArray() : requester.getBytes();
            Instant expires = pbRec.hasTtl() && pbRec.getTtl() > 0 ? Instant.now().plusSeconds(pbRec.getTtl()) : null;
            recordStore.put(new Record(key, value, publisher, expires));
        }
        return Dht.Message.newBuilder().setType(Dht.Message.MessageType.PUT_VALUE).setRecord(pbRec).build();
    }

    Dht.Message handleAddProvider(Dht.Message req, PeerId requester) {
        byte[] key = req.getKey().toByteArray();
        if (key.length == 0 || key.length > 80) return Dht.Message.newBuilder().setType(Dht.Message.MessageType.ADD_PROVIDER).build();
        if (!isValidProviderKey(key)) return Dht.Message.newBuilder().setType(Dht.Message.MessageType.ADD_PROVIDER).build();
        for (Dht.Message.Peer p : req.getProviderPeersList()) {
            PeerId providerId = new PeerId(p.getId().toByteArray());
            if (!providerId.equals(requester)) continue;
            List<Multiaddr> addrs = new ArrayList<>();
            for (ByteString ab : p.getAddrsList()) { try { addrs.add(Multiaddr.deserialize(ab.toByteArray())); } catch (Exception ignored) {} }
            if (providerStore != null) {
                providerStore.addProvider(new ProviderRecord(key, providerId, Instant.now().plus(providerRecordTTL), Instant.now().plus(providerAddrTTL), addrs));
            }
        }
        return Dht.Message.newBuilder().setType(Dht.Message.MessageType.ADD_PROVIDER).build();
    }

    Dht.Message handleGetProviders(Dht.Message req, PeerId requester) {
        byte[] key = req.getKey().toByteArray();
        Dht.Message.Builder builder = Dht.Message.newBuilder().setType(Dht.Message.MessageType.GET_PROVIDERS).setKey(ByteString.copyFrom(key));
        if (providerStore != null) {
            for (ProviderRecord pr : providerStore.getProviders(key)) {
                builder.addProviderPeers(Dht.Message.Peer.newBuilder()
                        .setId(ByteString.copyFrom(pr.getProvider().getBytes()))
                        .setConnection(Dht.Message.ConnectionType.CONNECTED)
                        .addAllAddrs(pr.getAliveAddresses().stream().map(a -> ByteString.copyFrom(a.serialize())).toList()));
            }
        }
        for (KadPeer p : routingTable.findClosest(key, kValue)) { if (!p.nodeId.equals(requester)) builder.addCloserPeers(toProtoPeer(p)); }
        return builder.build();
    }

    Dht.Message.Peer toProtoPeer(KadPeer p) {
        Dht.Message.Peer.Builder pb = Dht.Message.Peer.newBuilder()
                .setId(ByteString.copyFrom(p.nodeId.getBytes()))
                .setConnection(Dht.Message.ConnectionType.forNumber(p.connectionType.getValue()));
        for (Multiaddr addr : p.multiaddrs) pb.addAddrs(ByteString.copyFrom(addr.serialize()));
        return pb.build();
    }

    List<KadPeer> parseCloserPeers(Dht.Message msg) {
        List<KadPeer> peers = new ArrayList<>();
        for (Dht.Message.Peer p : msg.getCloserPeersList()) {
            try {
                PeerId nodeId = new PeerId(p.getId().toByteArray());
                List<Multiaddr> addrs = new ArrayList<>();
                for (ByteString ab : p.getAddrsList()) { try { addrs.add(Multiaddr.deserialize(ab.toByteArray())); } catch (Exception ignored) {} }
                peers.add(new KadPeer(nodeId, addrs, KadPeer.ConnectionType.fromValue(p.getConnection().getNumber())));
            } catch (Exception ignored) {}
        }
        return peers;
    }

    List<ProviderRecord> parseProviders(Dht.Message msg) {
        List<ProviderRecord> providers = new ArrayList<>();
        for (Dht.Message.Peer p : msg.getProviderPeersList()) {
            try {
                PeerId providerId = new PeerId(p.getId().toByteArray());
                List<Multiaddr> addrs = new ArrayList<>();
                for (ByteString ab : p.getAddrsList()) { try { addrs.add(Multiaddr.deserialize(ab.toByteArray())); } catch (Exception ignored) {} }
                providers.add(new ProviderRecord(msg.getKey().toByteArray(), providerId, Instant.now().plus(providerRecordTTL), Instant.now().plus(providerAddrTTL), addrs));
            } catch (Exception ignored) {}
        }
        return providers;
    }

    public record FindNodeResponse(List<KadPeer> closerPeers) {
        public static FindNodeResponse fromMessage(Dht.Message msg, Duration providerRecordTTL, Duration providerAddrTTL) {
            List<KadPeer> closer = new ArrayList<>();
            for (Dht.Message.Peer p : msg.getCloserPeersList()) {
                try {
                    PeerId nodeId = new PeerId(p.getId().toByteArray());
                    List<Multiaddr> addrs = new ArrayList<>();
                    for (ByteString ab : p.getAddrsList()) { try { addrs.add(Multiaddr.deserialize(ab.toByteArray())); } catch (Exception ignored) {} }
                    closer.add(new KadPeer(nodeId, addrs, KadPeer.ConnectionType.fromValue(p.getConnection().getNumber())));
                } catch (Exception ignored) {}
            }
            return new FindNodeResponse(closer);
        }
    }

    public record GetValueResponse(Optional<Record> record, List<KadPeer> closerPeers) {
        public static GetValueResponse fromMessage(Dht.Message msg, RecordStore store) {
            Optional<Record> rec = Optional.empty();
            if (msg.hasRecord()) {
                Dht.Record pbRec = msg.getRecord();
                if (!pbRec.getKey().isEmpty() && !pbRec.getValue().isEmpty()) {
                    byte[] publisher = pbRec.hasPublisher() ? pbRec.getPublisher().toByteArray() : null;
                    Instant expires = pbRec.hasTtl() && pbRec.getTtl() > 0 ? Instant.now().plusSeconds(pbRec.getTtl()) : null;
                    Record record = new Record(pbRec.getKey().toByteArray(), pbRec.getValue().toByteArray(), publisher, expires);
                    if (store != null) store.put(record);
                    rec = Optional.of(record);
                }
            }
            List<KadPeer> closer = new ArrayList<>();
            for (Dht.Message.Peer p : msg.getCloserPeersList()) {
                try {
                    PeerId nodeId = new PeerId(p.getId().toByteArray());
                    List<Multiaddr> addrs = new ArrayList<>();
                    for (ByteString ab : p.getAddrsList()) { try { addrs.add(Multiaddr.deserialize(ab.toByteArray())); } catch (Exception ignored) {} }
                    closer.add(new KadPeer(nodeId, addrs, KadPeer.ConnectionType.fromValue(p.getConnection().getNumber())));
                } catch (Exception ignored) {}
            }
            return new GetValueResponse(rec, closer);
        }
    }

    public record GetProvidersResponse(List<ProviderRecord> providers, List<KadPeer> closerPeers) {
        public static GetProvidersResponse fromMessage(Dht.Message msg, ProviderStore store, Duration providerRecordTTL, Duration providerAddrTTL) {
            List<ProviderRecord> provs = new ArrayList<>();
            for (Dht.Message.Peer p : msg.getProviderPeersList()) {
                try {
                    PeerId providerId = new PeerId(p.getId().toByteArray());
                    List<Multiaddr> addrs = new ArrayList<>();
                    for (ByteString ab : p.getAddrsList()) { try { addrs.add(Multiaddr.deserialize(ab.toByteArray())); } catch (Exception ignored) {} }
                    ProviderRecord pr = new ProviderRecord(msg.getKey().toByteArray(), providerId,
                            Instant.now().plus(providerRecordTTL), Instant.now().plus(providerAddrTTL), addrs);
                    provs.add(pr);
                    if (store != null) store.addProvider(pr);
                } catch (Exception ignored) {}
            }
            List<KadPeer> closer = new ArrayList<>();
            for (Dht.Message.Peer p : msg.getCloserPeersList()) {
                try {
                    PeerId nodeId = new PeerId(p.getId().toByteArray());
                    List<Multiaddr> addrs = new ArrayList<>();
                    for (ByteString ab : p.getAddrsList()) { try { addrs.add(Multiaddr.deserialize(ab.toByteArray())); } catch (Exception ignored) {} }
                    closer.add(new KadPeer(nodeId, addrs, KadPeer.ConnectionType.fromValue(p.getConnection().getNumber())));
                } catch (Exception ignored) {}
            }
            return new GetProvidersResponse(provs, closer);
        }
    }

    private boolean isValidProviderKey(byte[] key) {
        if (key.length == 0) return false;
        int pos = 0;
        long hashFuncCode = 0;
        int shift = 0;
        while (pos < key.length) {
            byte b = key[pos++];
            hashFuncCode |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
            if (shift > 63) return false;
        }
        if (pos >= key.length) return false;
        long digestLength = 0;
        shift = 0;
        while (pos < key.length) {
            byte b = key[pos++];
            digestLength |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
            if (shift > 63) return false;
        }
        if (digestLength < 0 || digestLength > Integer.MAX_VALUE) return false;
        return pos + (int)digestLength == key.length;
    }

    public interface KademliaController {
        CompletableFuture<Dht.Message> sendRequest(Dht.Message msg);
    }

    static class InitiatorHandler extends io.netty.channel.ChannelInboundHandlerAdapter implements KademliaController {
        private final Stream stream;
        private final LinkedBlockingDeque<CompletableFuture<Dht.Message>> pending = new LinkedBlockingDeque<>();
        InitiatorHandler(Stream stream) { this.stream = stream; }

        @Override
        public CompletableFuture<Dht.Message> sendRequest(Dht.Message msg) {
            CompletableFuture<Dht.Message> future = new CompletableFuture<>();
            pending.add(future);
            stream.writeAndFlush(msg);
            return future;
        }

        @Override
        public void channelRead(io.netty.channel.ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof Dht.Message dhtMsg) { CompletableFuture<Dht.Message> f = pending.poll(); if (f != null) f.complete(dhtMsg); }
            else ctx.fireChannelRead(msg);
        }

        @Override
        public void channelInactive(io.netty.channel.ChannelHandlerContext ctx) {
            pending.forEach(f -> f.completeExceptionally(new ConnectionClosedException()));
            pending.clear();
            ctx.fireChannelInactive();
        }
    }

    static class ResponderHandler extends io.netty.channel.ChannelInboundHandlerAdapter implements KademliaController {
        private final KademliaProtocol proto;
        private final Stream stream;
        ResponderHandler(KademliaProtocol proto, Stream stream) { this.proto = proto; this.stream = stream; }

        @Override
        public void channelRead(io.netty.channel.ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof Dht.Message dhtMsg) {
                Dht.Message response = switch (dhtMsg.getType()) {
                    case PING -> proto.handlePing();
                    case FIND_NODE -> proto.handleFindNode(dhtMsg, stream.remotePeerId());
                    case GET_VALUE -> proto.handleGetValue(dhtMsg, stream.remotePeerId());
                    case PUT_VALUE -> proto.handlePutValue(dhtMsg, stream.remotePeerId());
                    case ADD_PROVIDER -> proto.handleAddProvider(dhtMsg, stream.remotePeerId());
                    case GET_PROVIDERS -> proto.handleGetProviders(dhtMsg, stream.remotePeerId());
                    default -> null;
                };
                if (response != null) stream.writeAndFlush(response);
            } else ctx.fireChannelRead(msg);
        }

        @Override
        public CompletableFuture<Dht.Message> sendRequest(Dht.Message msg) {
            throw new UnsupportedOperationException("Responder cannot send requests");
        }
    }

    public static class KademliaCodec extends io.netty.channel.ChannelDuplexHandler {
        private final java.io.ByteArrayOutputStream readBuffer = new java.io.ByteArrayOutputStream(4096);
        private int expectedLength = -1;

        @Override
        public void write(io.netty.channel.ChannelHandlerContext ctx, Object msg, io.netty.channel.ChannelPromise promise) {
            if (msg instanceof Dht.Message dhtMsg) {
                byte[] payload = dhtMsg.toByteArray();
                if (payload.length > 16384) { promise.setFailure(new IllegalStateException("Message too large")); return; }
                byte[] varint = RpcCodec.encodeVarint(payload.length);
                io.netty.buffer.ByteBuf buf = ctx.alloc().buffer(varint.length + payload.length);
                buf.writeBytes(varint); buf.writeBytes(payload);
                ctx.writeAndFlush(buf, promise);
            } else ctx.write(msg, promise);
        }

        @Override
        public void channelRead(io.netty.channel.ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof io.netty.buffer.ByteBuf byteBuf) {
                try {
                    byte[] data = new byte[byteBuf.readableBytes()];
                    byteBuf.readBytes(data); byteBuf.release();
                    int offset = 0;
                    while (offset < data.length) {
                        if (expectedLength < 0) {
                            long result = 0; int shift = 0; boolean done = false;
                            while (offset < data.length && !done) {
                                byte b = data[offset++]; result |= (long)(b & 0x7F) << shift;
                                if ((b & 0x80) == 0) done = true; shift += 7;
                            }
                            if (!done) continue;
                            expectedLength = (int) result;
                            if (expectedLength > 16384) { ctx.fireExceptionCaught(new IllegalStateException("Frame too large")); return; }
                        } else {
                            int avail = data.length - offset;
                            int needed = expectedLength - readBuffer.size();
                            int toRead = Math.min(avail, needed);
                            readBuffer.write(data, offset, toRead); offset += toRead;
                            if (readBuffer.size() >= expectedLength) {
                                byte[] msgBytes = readBuffer.toByteArray(); readBuffer.reset(); expectedLength = -1;
                                ctx.fireChannelRead(Dht.Message.parseFrom(msgBytes));
                            }
                        }
                    }
                } catch (Exception e) { ctx.fireExceptionCaught(e); }
            } else ctx.fireChannelRead(msg);
        }
    }
}
