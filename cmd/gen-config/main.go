// Command gen-config prints a fully annotated broker configuration to stdout.
// Every field includes a companion _comment_ key that describes its meaning,
// allowed values, and the default. The output is valid JSON parseable by
// config.Load() because encoding/json ignores unknown fields when
// unmarshalling into a typed struct.
//
// Usage:
//
//	go run ./cmd/gen-config > broker.json
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
)

func main() {
	cfg := config.GenerateDefault()
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "gen-config: encode: %v\n", err)
		os.Exit(1)
	}
}
