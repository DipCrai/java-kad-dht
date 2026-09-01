# java-kad-dht

Full [Kademlia DHT](https://github.com/libp2p/specs/blob/master/kad-dht/README.md) implementation for [jvm-libp2p](https://github.com/libp2p/jvm-libp2p). Interoperable with Go and Rust libp2p nodes.

## What is this

jvm-libp2p ships with circuit relay, noise, yamux, identify, ping — but no Kademlia DHT. This library fills that gap.

Tested against:
- **Go**: go-libp2p v0.36.0 + go-libp2p-kad-dht v0.25.2
- **Rust**: libp2p v0.54.1 (libp2p-kad v0.46.2)

## Features

- All 6 Kad RPCs: PING, FIND_NODE, GET_VALUE, PUT_VALUE, ADD_PROVIDER, GET_PROVIDERS
- Iterative lookup engine with α/β parallelism and stall detection
- 256 k-bucket routing table with pending entries and replacement cache
- Record store with TTL, GC, replication, republish
- Provider store with batch flush, TTL
- Namespaced validators (/pk, /ipns, custom)
- Bootstrap + periodic routing table refresh
- Client/Server mode management
- IdentifyAdapter integration with jvm-libp2p
- Metrics counters

## Installation

### JitPack (recommended)

```groovy
repositories {
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation 'com.github.DipCrai:java-kad-dht:main-SNAPSHOT'
}
```

### Local

```bash
./gradlew publishToMavenLocal
```

```groovy
repositories {
    mavenLocal()
}

dependencies {
    implementation 'com.github.dipcrai:java-kad-dht:0.1.0'
}
```

## Quick start

```java
import com.libp2p.kademlia.KadDht;
import com.libp2p.kademlia.config.KadConfig;
import com.libp2p.kademlia.config.KadMode;
import io.libp2p.core.Host;
import io.libp2p.core.dsl.HostBuilder;

// Create host
Host host = new HostBuilder()
        .secureChannel(NoiseXXSecureChannel::new)
        .muxer(StreamMuxerProtocol::getYamux)
        .listen("/ip4/0.0.0.0/tcp/4001")
        .build();
host.start().join();

// Create DHT
KadDht dht = new KadDht(KadConfig.builder()
        .mode(KadMode.SERVER)
        .build());
dht.setHost(host);
dht.start();
dht.bootstrap();

// PUT
byte[] key = "my-key".getBytes();
byte[] value = "hello".getBytes();
dht.putValue(key, value).get();

// GET
Record record = dht.getValue(key).get();
```

## Configuration

```java
KadConfig config = KadConfig.builder()
        .mode(KadMode.SERVER)           // CLIENT, SERVER, AUTO, AUTO_SERVER
        .queryTimeout(Duration.ofSeconds(60))
        .writeQuorum(1)
        .readQuorum(1)
        .build();
```

### Client vs Server

| Mode | Accepts inbound | Advertises via Identify | Enters routing tables |
|------|----------------|------------------------|----------------------|
| SERVER | Yes | Yes | Yes |
| CLIENT | No | No | No |
| AUTO | Starts CLIENT, upgrades to SERVER when external address confirmed | | |
| AUTO_SERVER | Starts SERVER | | |

## Interop testing

```bash
# Build everything
./gradlew build -x test
cd interop/go && go build -o go-node . && cd ../..
cd interop/rust && cargo build --release && cd ../..

# Start nodes
./interop/go/go-node &
./interop/rust/target/release/kad-rust-node &

# Run interop tests
./gradlew test --tests "com.libp2p.kademlia.InteropDht.*" \
  -Dinterop.go="/ip4/127.0.0.1/tcp/PORT/p2p/PEER_ID" \
  -Dinterop.rust="/ip4/127.0.0.1/tcp/PORT/p2p/PEER_ID" \
  --rerun-tasks
```

### Test matrix

| Test | Go↔Java | Rust↔Java |
|------|---------|-----------|
| PING | ✅ | ✅ |
| FIND_NODE | ✅ | ✅ |
| PUT_VALUE → GET_VALUE | ✅ | ✅ |
| ADD_PROVIDER → GET_PROVIDERS | ✅ | ✅ |
| Multi-hop (2 Java → Go/Rust) | ✅ | ✅ |

## Architecture

```
com.libp2p.kademlia/
  KadDht.java                    — main facade, lifecycle, public API

  protocol/
    KadMessage.java              — protobuf message helpers
    RpcCodec.java                — varint framing encode/decode
    ProtocolHandler.java         — stream handler (initiator/responder)

  routing/
    RoutingTable.java            — 256 k-buckets
    KBucket.java                 — single bucket + pending entry
    KBucketEntry.java            — peer entry with timestamps
    ReplacementCache.java        — replacement entries for full buckets

  lookup/
    IterativeLookup.java         — core lookup state machine
    QueryScheduler.java          — manages concurrent queries

  rpc/
    PingRpc.java / FindNodeRpc.java / GetValueRpc.java
    PutValueRpc.java / AddProviderRpc.java / GetProvidersRpc.java

  records/
    Record.java / RecordStore.java / MemoryRecordStore.java
    RecordValidator.java / NamespacedValidator.java
    RecordRepublisher.java / ProviderStore.java

  config/
    KadConfig.java               — immutable config builder
    KadMode.java                 — CLIENT/SERVER/AUTO/AUTO_SERVER
```

## Wire protocol

Protocol ID: `/ipfs/kad/1.0.0`

Framing: varint length-delimited protobuf, max 16KB per message.

Each RPC: open stream → send request → receive response → close stream.

## Constants

| Name | Value |
|------|-------|
| K (bucket size) | 20 |
| α (query parallelism) | 3 |
| β (resiliency) | 3 |
| MAX_PACKET_SIZE | 16,384 |
| RECORD_TTL | 48h |
| PROVIDER_RECORD_TTL | 48h |
| BOOTSTRAP_INTERVAL | 5min |

## Dependencies

- `io.libp2p:jvm-libp2p:1.3.5-RELEASE`
- `com.google.protobuf:protobuf-java:3.25.1`
- `io.netty:netty-all:4.1.108.Final`


