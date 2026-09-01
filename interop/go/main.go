package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
)

func main() {
	protocolID := "/ipfs/kad/1.0.0"
	if len(os.Args) > 1 {
		protocolID = os.Args[1]
	}

	ctx := context.Background()

	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create host: %v\n", err)
		os.Exit(1)
	}
	defer h.Close()

	fmt.Fprintf(os.Stderr, "Host ID: %s\n", h.ID().String())
	fmt.Fprintf(os.Stderr, "Protocol: %s\n", protocolID)

	ds := dssync.MutexWrap(datastore.NewMapDatastore())

	kadDHT, err := dht.New(ctx, h,
		dht.V1ProtocolOverride(protocol.ID(protocolID)),
		dht.Mode(dht.ModeServer),
		dht.BucketSize(20),
		dht.Datastore(ds),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create DHT: %v\n", err)
		os.Exit(1)
	}
	defer kadDHT.Close()

	if err := kadDHT.Bootstrap(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to bootstrap DHT: %v\n", err)
		os.Exit(1)
	}

	time.Sleep(1 * time.Second)

	addrs := h.Addrs()
	if len(addrs) == 0 {
		fmt.Fprintf(os.Stderr, "no addresses\n")
		os.Exit(1)
	}

	for _, addr := range addrs {
		fullAddr, _ := multiaddr.NewMultiaddr(addr.String() + "/p2p/" + h.ID().String())
		fmt.Printf("%s\n", fullAddr.String())
	}
	fmt.Println("")

	fmt.Fprintf(os.Stderr, "DHT ready, listening...\n")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Fprintf(os.Stderr, "Shutting down...\n")
}
