package com.libp2p.kademlia;

import com.google.protobuf.ByteString;
import com.libp2p.kademlia.pb.Dht;
import io.libp2p.core.*;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.core.multistream.ProtocolBinding;
import io.libp2p.core.multistream.ProtocolDescriptor;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class KademliaProtocol implements ProtocolBinding<KademliaProtocol.KademliaController> {

    private final KademliaConfig config;
    private volatile RoutingTable routingTable;
    private volatile Host host;
    private volatile boolean running = false;
    private final ScheduledExecutorService scheduler;
    private volatile RecordStore recordStore;
    private volatile ProviderStore providerStore;
    private volatile RecordValidator validator = RecordValidator.NOOP;

    public KademliaProtocol(KademliaConfig config) {
        this.config = config;
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "kademlia");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public ProtocolDescriptor getProtocolDescriptor() {
        return new ProtocolDescriptor(config.protocolName);
    }

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

    public void setHost(Host host) {
        this.host = host;
        this.routingTable = new RoutingTable(host.getPeerId(), config.kValue, config.pendingTimeout);
    }

    public void setRecordStore(RecordStore store) { this.recordStore = store; }
    public void setProviderStore(ProviderStore store) { this.providerStore = store; }
    public void setValidator(RecordValidator validator) { this.validator = validator; }

    public CompletableFuture<Void> start() {
        if (running) return CompletableFuture.completedFuture(null);
        if (host == null) throw new IllegalStateException("setHost() first");
        running = true;
        return runBootstrap();
    }

    public void stop() {
        running = false;
        scheduler.shutdownNow();
    }

    public CompletableFuture<Void> runBootstrap() {
        return CompletableFuture.runAsync(() -> {
            if (config.bootstrapNodes == null) return;
            for (Multiaddr addr : config.bootstrapNodes) {
                try {
                    host.getNetwork().connect(addr).get(10, TimeUnit.SECONDS);
                } catch (Exception ignored) {}
            }
            byte[] selfKey = XorId.fromPeerId(host.getPeerId());
            List<KadPeer> initial = routingTable.findClosest(selfKey, config.kValue);
            for (KadPeer p : initial) {
                try {
                    sendFindNode(selfKey, p).get(config.substreamTimeout.toSeconds(), TimeUnit.SECONDS);
                } catch (Exception ignored) {}
            }
        });
    }

    public CompletableFuture<List<KadPeer>> sendFindNode(byte[] target, KadPeer peer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                StreamPromise<KademliaController> promise = host.newStream(List.of(config.protocolName), peer.nodeId);
                KademliaController ctrl = promise.getController().get(config.substreamTimeout.toSeconds(), TimeUnit.SECONDS);
                if (ctrl == null) return List.<KadPeer>of();

                Dht.Message req = Dht.Message.newBuilder()
                        .setType(Dht.Message.MessageType.FIND_NODE)
                        .setKey(ByteString.copyFrom(target))
                        .setClusterLevelRaw(10)
                        .build();

                Dht.Message resp = ctrl.sendRequest(req).get(config.substreamTimeout.toSeconds(), TimeUnit.SECONDS);
                return parseCloserPeers(resp);
            } catch (Exception e) {
                return List.<KadPeer>of();
            }
        });
    }

    public CompletableFuture<Boolean> sendPing(PeerId peer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                StreamPromise<KademliaController> promise = host.newStream(List.of(config.protocolName), peer);
                KademliaController ctrl = promise.getController().get(config.substreamTimeout.toSeconds(), TimeUnit.SECONDS);
                if (ctrl == null) return false;

                Dht.Message req = Dht.Message.newBuilder()
                        .setType(Dht.Message.MessageType.PING)
                        .build();

                Dht.Message resp = ctrl.sendRequest(req).get(config.substreamTimeout.toSeconds(), TimeUnit.SECONDS);
                return resp.getType() == Dht.Message.MessageType.PING;
            } catch (Exception e) {
                return false;
            }
        });
    }

    public CompletableFuture<Boolean> sendGetValue(byte[] key, PeerId peer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                StreamPromise<KademliaController> promise = host.newStream(List.of(config.protocolName), peer);
                KademliaController ctrl = promise.getController().get(config.substreamTimeout.toSeconds(), TimeUnit.SECONDS);
                if (ctrl == null) return false;

                Dht.Message req = Dht.Message.newBuilder()
                        .setType(Dht.Message.MessageType.GET_VALUE)
                        .setKey(ByteString.copyFrom(key))
                        .setClusterLevelRaw(10)
                        .build();

                Dht.Message resp = ctrl.sendRequest(req).get(config.substreamTimeout.toSeconds(), TimeUnit.SECONDS);
                if (resp.hasRecord() && recordStore != null) {
                    Dht.Record pbRec = resp.getRecord();
                    Record record = new Record(pbRec.getKey().toByteArray(), pbRec.getValue().toByteArray(),
                            pbRec.hasPublisher() ? pbRec.getPublisher().toByteArray() : null,
                            pbRec.hasTtl() && pbRec.getTtl() > 0 ? java.time.Instant.now().plusSeconds(pbRec.getTtl()) : null);
                    recordStore.put(record);
                }
                return resp.hasRecord();
            } catch (Exception e) {
                return false;
            }
        });
    }

    public CompletableFuture<Boolean> sendPutValue(Record record, PeerId peer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                StreamPromise<KademliaController> promise = host.newStream(List.of(config.protocolName), peer);
                KademliaController ctrl = promise.getController().get(config.substreamTimeout.toSeconds(), TimeUnit.SECONDS);
                if (ctrl == null) return false;

                Dht.Record pbRec = Dht.Record.newBuilder()
                        .setKey(ByteString.copyFrom(record.getKey()))
                        .setValue(ByteString.copyFrom(record.getValue()))
                        .setTimeReceived(java.time.Instant.now().toString())
                        .build();

                Dht.Message req = Dht.Message.newBuilder()
                        .setType(Dht.Message.MessageType.PUT_VALUE)
                        .setRecord(pbRec)
                        .setClusterLevelRaw(10)
                        .build();

                Dht.Message resp = ctrl.sendRequest(req).get(config.substreamTimeout.toSeconds(), TimeUnit.SECONDS);
                return resp.hasRecord() && Arrays.equals(resp.getRecord().getKey().toByteArray(), record.getKey());
            } catch (Exception e) {
                return false;
            }
        });
    }

    public CompletableFuture<Boolean> sendAddProvider(byte[] key, PeerId peer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                StreamPromise<KademliaController> promise = host.newStream(List.of(config.protocolName), peer);
                KademliaController ctrl = promise.getController().get(config.substreamTimeout.toSeconds(), TimeUnit.SECONDS);
                if (ctrl == null) return false;

                Dht.Message.Peer selfPeer = Dht.Message.Peer.newBuilder()
                        .setId(ByteString.copyFrom(host.getPeerId().getBytes()))
                        .setConnection(Dht.Message.ConnectionType.CONNECTED)
                        .build();

                Dht.Message req = Dht.Message.newBuilder()
                        .setType(Dht.Message.MessageType.ADD_PROVIDER)
                        .setKey(ByteString.copyFrom(key))
                        .addProviderPeers(selfPeer)
                        .setClusterLevelRaw(10)
                        .build();

                ctrl.sendRequest(req).get(config.substreamTimeout.toSeconds(), TimeUnit.SECONDS);
                return true;
            } catch (Exception e) {
                return false;
            }
        });
    }

    public CompletableFuture<Boolean> sendGetProviders(byte[] key, PeerId peer) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                StreamPromise<KademliaController> promise = host.newStream(List.of(config.protocolName), peer);
                KademliaController ctrl = promise.getController().get(config.substreamTimeout.toSeconds(), TimeUnit.SECONDS);
                if (ctrl == null) return false;

                Dht.Message req = Dht.Message.newBuilder()
                        .setType(Dht.Message.MessageType.GET_PROVIDERS)
                        .setKey(ByteString.copyFrom(key))
                        .setClusterLevelRaw(10)
                        .build();

                Dht.Message resp = ctrl.sendRequest(req).get(config.substreamTimeout.toSeconds(), TimeUnit.SECONDS);
                return resp.getProviderPeersCount() > 0;
            } catch (Exception e) {
                return false;
            }
        });
    }

    Dht.Message handlePing(Dht.Message req) {
        return Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.PING)
                .build();
    }

    Dht.Message handleFindNode(Dht.Message req, PeerId requester) {
        byte[] key = req.getKey().toByteArray();
        List<KadPeer> closer = routingTable.findClosest(key, config.kValue);
        Dht.Message.Builder builder = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.FIND_NODE)
                .setKey(ByteString.copyFrom(key));
        for (KadPeer p : closer) {
            if (p.nodeId.equals(requester)) continue;
            builder.addCloserPeers(toProtoPeer(p));
        }
        return builder.build();
    }

    Dht.Message handleGetValue(Dht.Message req, PeerId requester) {
        byte[] key = req.getKey().toByteArray();
        Dht.Message.Builder builder = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.GET_VALUE)
                .setKey(ByteString.copyFrom(key));

        if (recordStore != null) {
            Record record = recordStore.get(key);
            if (record != null) {
                Dht.Record pbRec = Dht.Record.newBuilder()
                        .setKey(ByteString.copyFrom(record.getKey()))
                        .setValue(ByteString.copyFrom(record.getValue()))
                        .setTimeReceived(record.getTimeReceived() != null ? record.getTimeReceived().toString() : "")
                        .build();
                builder.setRecord(pbRec);
            }
        }

        List<KadPeer> closer = routingTable.findClosest(key, config.kValue);
        for (KadPeer p : closer) {
            if (p.nodeId.equals(requester)) continue;
            builder.addCloserPeers(toProtoPeer(p));
        }
        return builder.build();
    }

    Dht.Message handlePutValue(Dht.Message req, PeerId requester) {
        if (!req.hasRecord()) return Dht.Message.newBuilder().setType(Dht.Message.MessageType.PUT_VALUE).build();

        Dht.Record pbRec = req.getRecord();
        if (pbRec.getKey().isEmpty()) return Dht.Message.newBuilder().setType(Dht.Message.MessageType.PUT_VALUE).build();

        byte[] key = pbRec.getKey().toByteArray();
        byte[] value = pbRec.getValue().toByteArray();

        if (recordStore != null && validator.validate(key, value)) {
            Record record = new Record(key, value,
                    pbRec.hasPublisher() ? pbRec.getPublisher().toByteArray() : requester.getBytes(),
                    pbRec.hasTtl() && pbRec.getTtl() > 0 ? java.time.Instant.now().plusSeconds(pbRec.getTtl()) : null);
            recordStore.put(record);
        }

        return Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.PUT_VALUE)
                .setRecord(pbRec)
                .build();
    }

    Dht.Message handleAddProvider(Dht.Message req, PeerId requester) {
        byte[] key = req.getKey().toByteArray();
        if (key.length == 0 || key.length > 80) {
            return Dht.Message.newBuilder().setType(Dht.Message.MessageType.ADD_PROVIDER).build();
        }

        for (Dht.Message.Peer p : req.getProviderPeersList()) {
            PeerId providerId = new PeerId(p.getId().toByteArray());
            if (!providerId.equals(requester)) continue;

            List<Multiaddr> addrs = new ArrayList<>();
            for (ByteString ab : p.getAddrsList()) {
                try { addrs.add(Multiaddr.deserialize(ab.toByteArray())); } catch (Exception ignored) {}
            }

            if (providerStore != null) {
                ProviderRecord pr = new ProviderRecord(key, providerId,
                        java.time.Instant.now().plus(config.providerRecordTTL), addrs);
                providerStore.addProvider(pr);
            }
        }

        return Dht.Message.newBuilder().setType(Dht.Message.MessageType.ADD_PROVIDER).build();
    }

    Dht.Message handleGetProviders(Dht.Message req, PeerId requester) {
        byte[] key = req.getKey().toByteArray();
        Dht.Message.Builder builder = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.GET_PROVIDERS)
                .setKey(ByteString.copyFrom(key));

        if (providerStore != null) {
            List<ProviderRecord> providers = providerStore.getProviders(key);
            for (ProviderRecord pr : providers) {
                Dht.Message.Peer.Builder pb = Dht.Message.Peer.newBuilder()
                        .setId(ByteString.copyFrom(pr.getProvider().getBytes()))
                        .setConnection(Dht.Message.ConnectionType.CONNECTED);
                for (Multiaddr addr : pr.getAddresses()) {
                    pb.addAddrs(ByteString.copyFrom(addr.serialize()));
                }
                builder.addProviderPeers(pb);
            }
        }

        List<KadPeer> closer = routingTable.findClosest(key, config.kValue);
        for (KadPeer p : closer) {
            if (p.nodeId.equals(requester)) continue;
            builder.addCloserPeers(toProtoPeer(p));
        }
        return builder.build();
    }

    private Dht.Message.Peer toProtoPeer(KadPeer p) {
        Dht.Message.Peer.Builder pb = Dht.Message.Peer.newBuilder()
                .setId(ByteString.copyFrom(XorId.fromPeerId(p.nodeId)))
                .setConnection(Dht.Message.ConnectionType.forNumber(p.connectionType.value));
        for (Multiaddr addr : p.multiaddrs) {
            pb.addAddrs(ByteString.copyFrom(addr.serialize()));
        }
        return pb.build();
    }

    private List<KadPeer> parseCloserPeers(Dht.Message msg) {
        List<KadPeer> peers = new ArrayList<>();
        for (Dht.Message.Peer p : msg.getCloserPeersList()) {
            try {
                PeerId nodeId = new PeerId(p.getId().toByteArray());
                List<Multiaddr> addrs = new ArrayList<>();
                for (ByteString ab : p.getAddrsList()) {
                    try { addrs.add(Multiaddr.deserialize(ab.toByteArray())); } catch (Exception ignored) {}
                }
                peers.add(new KadPeer(nodeId, addrs, KadPeer.ConnectionType.fromValue(p.getConnection().getNumber())));
            } catch (Exception ignored) {}
        }
        return peers;
    }

    public RoutingTable getRoutingTable() { return routingTable; }
    public Host getHost() { return host; }
    public boolean isRunning() { return running; }
    public KademliaConfig getConfig() { return config; }

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
            if (msg instanceof Dht.Message dhtMsg) {
                CompletableFuture<Dht.Message> f = pending.poll();
                if (f != null) f.complete(dhtMsg);
            } else {
                ctx.fireChannelRead(msg);
            }
        }

        @Override
        public void channelInactive(io.netty.channel.ChannelHandlerContext ctx) {
            pending.forEach(f -> f.completeExceptionally(new ConnectionClosedException()));
            pending.clear();
            ctx.fireChannelInactive();
        }
    }

    static class ResponderHandler extends io.netty.channel.ChannelInboundHandlerAdapter implements KademliaController {
        private final KademliaProtocol kademlia;
        private final Stream stream;

        ResponderHandler(KademliaProtocol kademlia, Stream stream) {
            this.kademlia = kademlia;
            this.stream = stream;
        }

        @Override
        public void channelRead(io.netty.channel.ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof Dht.Message dhtMsg) {
                Dht.Message response = switch (dhtMsg.getType()) {
                    case PING -> kademlia.handlePing(dhtMsg);
                    case FIND_NODE -> kademlia.handleFindNode(dhtMsg, stream.remotePeerId());
                    case GET_VALUE -> kademlia.handleGetValue(dhtMsg, stream.remotePeerId());
                    case PUT_VALUE -> kademlia.handlePutValue(dhtMsg, stream.remotePeerId());
                    case ADD_PROVIDER -> kademlia.handleAddProvider(dhtMsg, stream.remotePeerId());
                    case GET_PROVIDERS -> kademlia.handleGetProviders(dhtMsg, stream.remotePeerId());
                    default -> null;
                };
                if (response != null) {
                    stream.writeAndFlush(response);
                }
            } else {
                ctx.fireChannelRead(msg);
            }
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
                if (payload.length > 16384) {
                    promise.setFailure(new IllegalStateException("Message too large: " + payload.length));
                    return;
                }
                byte[] varint = encodeVarint(payload.length);
                io.netty.buffer.ByteBuf buf = ctx.alloc().buffer(varint.length + payload.length);
                buf.writeBytes(varint);
                buf.writeBytes(payload);
                ctx.writeAndFlush(buf, promise);
            } else {
                ctx.write(msg, promise);
            }
        }

        @Override
        public void channelRead(io.netty.channel.ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof io.netty.buffer.ByteBuf byteBuf) {
                try {
                    byte[] data = new byte[byteBuf.readableBytes()];
                    byteBuf.readBytes(data);
                    byteBuf.release();

                    int offset = 0;
                    while (offset < data.length) {
                        if (expectedLength < 0) {
                            long result = 0;
                            int shift = 0;
                            boolean done = false;
                            while (offset < data.length && !done) {
                                byte b = data[offset++];
                                result |= (long) (b & 0x7F) << shift;
                                if ((b & 0x80) == 0) done = true;
                                shift += 7;
                            }
                            if (!done) continue;
                            expectedLength = (int) result;
                            if (expectedLength > 16384) {
                                ctx.fireExceptionCaught(new IllegalStateException("Frame too large: " + expectedLength));
                                return;
                            }
                        } else {
                            int available = data.length - offset;
                            int needed = expectedLength - readBuffer.size();
                            int toRead = Math.min(available, needed);
                            readBuffer.write(data, offset, toRead);
                            offset += toRead;

                            if (readBuffer.size() >= expectedLength) {
                                byte[] msgBytes = readBuffer.toByteArray();
                                readBuffer.reset();
                                expectedLength = -1;
                                Dht.Message dhtMsg = Dht.Message.parseFrom(msgBytes);
                                ctx.fireChannelRead(dhtMsg);
                            }
                        }
                    }
                } catch (Exception e) {
                    ctx.fireExceptionCaught(e);
                }
            } else {
                ctx.fireChannelRead(msg);
            }
        }

        static byte[] encodeVarint(int value) {
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            while ((value & ~0x7F) != 0) {
                out.write((value & 0x7F) | 0x80);
                value >>>= 7;
            }
            out.write(value);
            return out.toByteArray();
        }
    }
}
