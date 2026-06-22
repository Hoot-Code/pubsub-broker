// Command gateway starts the optional HTTP/WebSocket gateway as a separate
// process, connecting to an already-running broker over the binary protocol.
//
// Usage:
//
//	gateway -broker-addr 127.0.0.1:9000 -addr :8080
//
// This is an alternative to running the gateway embedded inside the main
// broker process (set "gateway": {"enabled": true, "addr": ":8080"} in
// broker.json instead). Run exactly one of the two, not both, against the
// same broker.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/gateway"
	"github.com/Hoot-Code/pubsub-broker/internal/logging"
)

func main() {
	brokerAddr := flag.String("broker-addr", "127.0.0.1:9000", "Address of the broker to connect to")
	addr := flag.String("addr", ":8080", "Address for the gateway HTTP/WebSocket server to listen on")
	flag.Parse()

	log := logging.Default()

	gw := gateway.NewGateway(config.GatewayConfig{Enabled: true, Addr: *addr}, *brokerAddr, nil, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Info("gateway stopping")
		cancel()
	}()

	log.Info("gateway starting", "addr", *addr, "broker_addr", *brokerAddr)
	startErr := gw.Start(ctx)
	if startErr != nil && startErr != context.Canceled {
		log.Error("gateway: exited with error", "err", startErr)
		os.Exit(1)
	}
	fmt.Println("gateway stopped")
}
