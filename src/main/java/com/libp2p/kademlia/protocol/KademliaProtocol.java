package com.libp2p.kademlia.protocol;

import com.google.protobuf.ByteString;
import com.libp2p.kademlia.pb.Dht;
import com.libp2p.kademlia.records.Record;
import com.libp2p.kademlia.records.RecordStore;
import com.libp2p.kademlia.records.RecordValidator;
import com.libp2p.kademlia.records.ProviderStore;
import com.libp2p.kademlia.records.ProviderRecord;
import com.libp2p.kademlia.routing.RoutingTable;
import com.libp2p.kademlia.routing.KadPeer;
import com.libp2p.kademlia.XorId;
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
    private volatile RoutingTable routingTable;
    private volatile Host host;
    private volatile RecordStore recordStore;
    private volatile ProviderStore providerStore;

    public KademliaProtocol(String protocolName, int kValue, Duration substreamTimeout, Duration providerRecordTTL) {
        this.protocolName = protocolName;
        this.kValue = kValue;
        this.substreamTimeout = substreamTimeout;
        this.providerRecordTTL = providerRecordTTL;
    }

    @Override
    public ProtocolDescriptor getProtocolDescriptor() { return new ProtocolDescriptor(protocolName); }

    @Override
    public CompletableFuture<KademliaController> initChannel(P2PChannel ch, String selectedProtocol) {
        Stream stream = (Stream) ch;
        if (!stream.isInitiator()) {
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

    public CompletableFuture<List<KadPeer>> sendFindNode(byte[] key, PeerId peer) {
        return sendAndParse(key, peer, RpcCodec.findNode(key), this::parseCloserPeers);
    }

    public CompletableFuture<Boolean> sendPing(PeerId peer) {
        return sendAndParse(null, peer, RpcCodec.ping(), msg -> msg.getType() == Dht.Message.MessageType.PING);
    }

    public CompletableFuture<Boolean> sendGetValue(byte[] key, PeerId peer) {
        return sendAndParse(key, peer, RpcCodec.getValue(key), msg -> {
            if (msg.hasRecord() && recordStore != null) {
                Dht.Record pbRec = msg.getRecord();
                Record record = new Record(pbRec.getKey().toByteArray(), pbRec.getValue().toByteArray(),
                        pbRec.hasPublisher() ? pbRec.getPublisher().toByteArray() : null,
                        pbRec.hasTtl() && pbRec.getTtl() > 0 ? Instant.now().plusSeconds(pbRec.getTtl()) : null);
                recordStore.put(record);
            }
            return msg.hasRecord();
        });
    }

    public CompletableFuture<Boolean> sendPutValue(Record record, PeerId peer) {
        return sendAndParse(record.getKey(), peer, RpcCodec.putValue(record), msg ->
                msg.hasRecord() && Arrays.equals(msg.getRecord().getKey().toByteArray(), record.getKey()));
    }

    public CompletableFuture<Boolean> sendAddProvider(byte[] key, PeerId peer) {
        return sendAndParse(key, peer, RpcCodec.addProvider(key, host.getPeerId().getBytes()), msg -> true);
    }

    public CompletableFuture<Boolean> sendGetProviders(byte[] key, PeerId peer) {
        return sendAndParse(key, peer, RpcCodec.getProviders(key), msg -> msg.getProviderPeersCount() > 0);
    }

    private <T> CompletableFuture<T> sendAndParse(byte[] key, PeerId peer, Dht.Message req, java.util.function.Function<Dht.Message, T> parser) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                StreamPromise<KademliaController> promise = host.newStream(List.of(protocolName), peer);
                KademliaController ctrl = promise.getController().get(substreamTimeout.toSeconds(), TimeUnit.SECONDS);
                if (ctrl == null) throw new IllegalStateException("No controller");
                Dht.Message resp = ctrl.sendRequest(req).get(substreamTimeout.toSeconds(), TimeUnit.SECONDS);
                return parser.apply(resp);
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        });
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
                builder.setRecord(Dht.Record.newBuilder().setKey(ByteString.copyFrom(record.getKey()))
                        .setValue(ByteString.copyFrom(record.getValue()))
                        .setTimeReceived(record.getTimeReceived() != null ? record.getTimeReceived().toString() : "").build());
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
            Record record = new Record(key, value,
                    pbRec.hasPublisher() ? pbRec.getPublisher().toByteArray() : requester.getBytes(),
                    pbRec.hasTtl() && pbRec.getTtl() > 0 ? Instant.now().plusSeconds(pbRec.getTtl()) : null);
            recordStore.put(record);
        }
        return Dht.Message.newBuilder().setType(Dht.Message.MessageType.PUT_VALUE).setRecord(pbRec).build();
    }

    Dht.Message handleAddProvider(Dht.Message req, PeerId requester) {
        byte[] key = req.getKey().toByteArray();
        if (key.length == 0 || key.length > 80) return Dht.Message.newBuilder().setType(Dht.Message.MessageType.ADD_PROVIDER).build();
        for (Dht.Message.Peer p : req.getProviderPeersList()) {
            PeerId providerId = new PeerId(p.getId().toByteArray());
            if (!providerId.equals(requester)) continue;
            List<Multiaddr> addrs = new ArrayList<>();
            for (ByteString ab : p.getAddrsList()) { try { addrs.add(Multiaddr.deserialize(ab.toByteArray())); } catch (Exception ignored) {} }
            if (providerStore != null) {
                providerStore.addProvider(new ProviderRecord(key, providerId, Instant.now().plus(providerRecordTTL), addrs));
            }
        }
        return Dht.Message.newBuilder().setType(Dht.Message.MessageType.ADD_PROVIDER).build();
    }

    Dht.Message handleGetProviders(Dht.Message req, PeerId requester) {
        byte[] key = req.getKey().toByteArray();
        Dht.Message.Builder builder = Dht.Message.newBuilder().setType(Dht.Message.MessageType.GET_PROVIDERS).setKey(ByteString.copyFrom(key));
        if (providerStore != null) {
            for (ProviderRecord pr : providerStore.getProviders(key)) {
                Dht.Message.Peer.Builder pb = Dht.Message.Peer.newBuilder().setId(ByteString.copyFrom(pr.getProvider().getBytes())).setConnection(Dht.Message.ConnectionType.CONNECTED);
                for (Multiaddr addr : pr.getAddresses()) pb.addAddrs(ByteString.copyFrom(addr.serialize()));
                builder.addProviderPeers(pb);
            }
        }
        for (KadPeer p : routingTable.findClosest(key, kValue)) { if (!p.nodeId.equals(requester)) builder.addCloserPeers(toProtoPeer(p)); }
        return builder.build();
    }

    private Dht.Message.Peer toProtoPeer(KadPeer p) {
        Dht.Message.Peer.Builder pb = Dht.Message.Peer.newBuilder()
                .setId(ByteString.copyFrom(XorId.fromPeerId(p.nodeId)))
                .setConnection(Dht.Message.ConnectionType.forNumber(p.connectionType.getValue()));
        for (Multiaddr addr : p.multiaddrs) pb.addAddrs(ByteString.copyFrom(addr.serialize()));
        return pb.build();
    }

    private List<KadPeer> parseCloserPeers(Dht.Message msg) {
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
