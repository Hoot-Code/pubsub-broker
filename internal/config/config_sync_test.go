package config_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
)

func TestCommittedConfigMatchesGenConfig(t *testing.T) {
	generated := config.GenerateDefault()

	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")
	committedPath := filepath.Join(repoRoot, "configs", "broker.json")

	data, err := os.ReadFile(committedPath)
	if err != nil {
		t.Fatalf("read committed config: %v", err)
	}

	var committed map[string]interface{}
	if err := json.Unmarshal(data, &committed); err != nil {
		t.Fatalf("parse committed config: %v", err)
	}

	// Compare top-level keys and values (ignoring ordering).
	var diffs []string
	for key, genVal := range generated {
		comVal, ok := committed[key]
		if !ok {
			diffs = append(diffs, key+": present in generated but missing from committed")
			continue
		}
		genJSON, _ := json.Marshal(genVal)
		comJSON, _ := json.Marshal(comVal)
		if string(genJSON) != string(comJSON) {
			diffs = append(diffs, key+": committed="+string(comJSON)+", generated="+string(genJSON))
		}
	}
	for key := range committed {
		if _, ok := generated[key]; !ok {
			diffs = append(diffs, key+": present in committed but missing from generated")
		}
	}

	if len(diffs) > 0 {
		t.Fatalf("configs/broker.json is out of sync with gen-config defaults:\n  %s\n\nRun: go run ./cmd/gen-config > configs/broker.json", joinDiffs(diffs))
	}
}

func joinDiffs(diffs []string) string {
	out := ""
	for i, d := range diffs {
		if i > 0 {
			out += "\n  "
		}
		out += d
	}
	return out
}
