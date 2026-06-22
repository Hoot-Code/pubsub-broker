// Command broker starts the distributed pub/sub broker.
//
// Usage:
//
//	broker -config /path/to/broker.json
//
// Environment variables:
//
//	BROKER_DATA_DIR  — base path for WAL, segment, and offset files.
//	                   Overrides the storage paths in the config file when set.
//	POD_NAME         — Kubernetes pod name; used as ClusterConfig.NodeID for
//	                   stable node identity across restarts (E4).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/broker"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/logging"
)

func main() {
	cfgPath := flag.String("config", "configs/broker.json", "Path to broker config file")
	flag.Parse()

	log := logging.Default()

	loader, err := config.Load(*cfgPath)
	if err != nil {
		log.Error("failed to load config", "path", *cfgPath, "err", err)
		os.Exit(1)
	}

	// E1: apply BROKER_DATA_DIR environment variable to storage paths.
	if dataDir := os.Getenv("BROKER_DATA_DIR"); dataDir != "" {
		cfg := loader.Get()
		cfg.Storage.WALPath = filepath.Join(dataDir, "wal")
		cfg.Storage.DataPath = filepath.Join(dataDir, "segments")
		// Write a temporary config with the overridden paths and reload.
		tmp, tmpErr := os.CreateTemp("", "broker-env-cfg-*.json")
		if tmpErr != nil {
			log.Error("failed to create temp config", "err", tmpErr)
			os.Exit(1)
		}
		tmpPath := tmp.Name()
		defer os.Remove(tmpPath)
		if encErr := json.NewEncoder(tmp).Encode(cfg); encErr != nil {
			tmp.Close()
			log.Error("failed to write temp config", "err", encErr)
			os.Exit(1)
		}
		tmp.Close()
		loader.Close()
		loader, err = config.Load(tmpPath)
		if err != nil {
			log.Error("failed to reload config with env overrides", "err", err)
			os.Exit(1)
		}
	}

	// E4: use POD_NAME as cluster NodeID for stable Kubernetes identity.
	if podName := os.Getenv("POD_NAME"); podName != "" {
		cfg := loader.Get()
		if cfg.Cluster.NodeID == "" {
			cfg.Cluster.NodeID = podName
		}
		if cfg.Broker.NodeID == "" || cfg.Broker.NodeID == "node-1" {
			cfg.Broker.NodeID = podName
		}
	}

	b, err := broker.New(loader)
	if err != nil {
		log.Error("failed to create broker", "err", err)
		os.Exit(1)
	}

	// Start broker in background goroutine so we can listen for signals.
	errC := make(chan error, 1)
	go func() { errC <- b.Start() }()

	// Graceful shutdown on SIGINT / SIGTERM (D1).
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigC:
		log.Info("received signal; shutting down", "signal", sig)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := b.Stop(ctx); err != nil {
			log.Error("graceful shutdown error", "err", err)
			os.Exit(1)
		}
		log.Info("broker stopped cleanly")

	case err := <-errC:
		if err != nil {
			log.Error("broker error", "err", err)
			fmt.Fprintf(os.Stderr, "broker error: %v\n", err)
			os.Exit(1)
		}
	}
}
