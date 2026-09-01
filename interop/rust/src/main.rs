use std::env;
use std::time::Duration;

use futures::StreamExt;
use libp2p::{identity, kad, noise, ping, identify, swarm::SwarmEvent, tcp, yamux, Multiaddr, PeerId, SwarmBuilder};
use libp2p::swarm::StreamProtocol;

#[derive(libp2p::swarm::NetworkBehaviour)]
struct MyBehaviour {
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    ping: ping::Behaviour,
    identify: identify::Behaviour,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    let protocol_id = env::args()
        .nth(1)
        .unwrap_or_else(|| "/ipfs/kad/1.0.0".to_string());

    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(local_key.public());
    eprintln!("Host ID: {}", local_peer_id);
    eprintln!("Protocol: {}", protocol_id);

    let mut swarm = SwarmBuilder::with_existing_identity(local_key.clone())
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .expect("Failed to create TCP transport")
        .with_behaviour(|key| {
            let peer_id = key.public().to_peer_id();
            let store = kad::store::MemoryStore::new(peer_id);
            let proto = StreamProtocol::try_from_owned(protocol_id.clone()).unwrap();
            let config = kad::Config::new(proto);
            let mut kademlia = kad::Behaviour::with_config(peer_id, store, config);
            kademlia.set_mode(Some(kad::Mode::Server));

            let ping = ping::Behaviour::new(ping::Config::new());
            let identify = identify::Behaviour::new(identify::Config::new(
                "/kad-rust-interop/0.1.0".to_string(),
                key.public(),
            ));

            Ok(MyBehaviour { kademlia, ping, identify })
        })
        .expect("Failed to build swarm")
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    let addr: Multiaddr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();
    swarm.listen_on(addr).expect("Failed to listen");

    loop {
        tokio::select! {
            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        let full_addr = address.with(libp2p::multiaddr::Protocol::P2p(local_peer_id.into()));
                        println!("{}", full_addr);
                        std::io::Write::flush(&mut std::io::stdout()).unwrap();
                        eprintln!("Listening on {}", full_addr);
                    }
                    SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                        eprintln!("CONNECTION ESTABLISHED: peer={}, endpoint={:?}", peer_id, endpoint);
                    }
                    SwarmEvent::ConnectionClosed { peer_id, .. } => {
                        eprintln!("CONNECTION CLOSED: peer={}", peer_id);
                    }
                    SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                        eprintln!("OUTGOING CONN ERROR: peer={:?}, error={}", peer_id, error);
                    }
                    SwarmEvent::IncomingConnectionError { local_addr, send_back_addr, error, .. } => {
                        eprintln!("INCOMING CONN ERROR: local={}, sendback={}, error={}", local_addr, send_back_addr, error);
                    }
                    SwarmEvent::Dialing { peer_id, .. } => {
                        eprintln!("DIALING: peer={:?}", peer_id);
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Kademlia(kad::Event::InboundRequest { request })) => {
                        eprintln!("KAD INBOUND: {:?}", std::mem::discriminant(&request));
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Kademlia(kad::Event::OutboundQueryProgressed { result, .. })) => {
                        eprintln!("KAD OUTBOUND: {:?}", result);
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Ping(e)) => {
                        eprintln!("PING: peer={}, result={:?}", e.peer, e.result);
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Identify(identify::Event::Received { .. })) => {
                        eprintln!("IDENTIFY received");
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Identify(identify::Event::Sent { .. })) => {
                        eprintln!("IDENTIFY sent");
                    }
                    SwarmEvent::Behaviour(MyBehaviourEvent::Identify(identify::Event::Error { error, .. })) => {
                        eprintln!("IDENTIFY error: {}", error);
                    }
                    e => {
                        eprintln!("OTHER EVENT: {:?}", std::mem::discriminant(&e));
                    }
                }
            }
        }
    }
}
