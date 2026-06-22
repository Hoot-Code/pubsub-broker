//go:build docker_smoke

package integration_test

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	proto "github.com/Hoot-Code/pubsub-broker/pkg/protocol"
)

func TestDockerSmoke(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not on PATH; skipping smoke test")
	}

	absRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}

	buildTag := "pubsub-broker-smoketest"
	containerName := buildTag

	t.Cleanup(func() {
		exec.Command("docker", "stop", "-t", "2", containerName).Run()
		exec.Command("docker", "rm", "-f", containerName).Run()
	})

	build := exec.Command("docker", "build", "-t", buildTag, ".")
	build.Dir = absRoot
	build.Env = append(os.Environ(), "DOCKER_BUILDKIT=0")
	out, err := build.CombinedOutput()
	if err != nil {
		t.Fatalf("docker build failed:\n%s", out)
	}

	tcpPort := pickPort(t)
	httpPort := pickPort(t)

	cmd := exec.Command("docker", "run", "-d",
		"--name", containerName,
		"-p", fmt.Sprintf("%d:9000", tcpPort),
		"-p", fmt.Sprintf("%d:9001", httpPort),
		"-e", "BROKER_DATA_DIR=/data",
		buildTag,
	)
	out, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker run failed:\n%s", out)
	}

	t.Cleanup(func() {
		exec.Command("docker", "stop", "-t", "2", containerName).Run()
		exec.Command("docker", "rm", "-f", containerName).Run()
	})

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", httpPort), 500*time.Millisecond)
		if err == nil {
			resp.Close()
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/health", httpPort)
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		logs := dockerLogs(t, containerName)
		t.Fatalf("health check failed: %v\n\nContainer logs:\n%s", err, logs)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logs := dockerLogs(t, containerName)
		t.Fatalf("health check: want HTTP 200, got %d\n\nContainer logs:\n%s", resp.StatusCode, logs)
	}

	t.Logf("Docker smoke test passed: HTTP health OK on port %d", httpPort)

	tcpAddr := fmt.Sprintf("127.0.0.1:%d", tcpPort)
	tcpConn, err := net.DialTimeout("tcp", tcpAddr, 3*time.Second)
	if err != nil {
		logs := dockerLogs(t, containerName)
		t.Fatalf("TCP connect to %s failed: %v\n\nContainer logs:\n%s", tcpAddr, err, logs)
	}
	defer tcpConn.Close()

	enc := proto.NewEncoder(tcpConn)
	if err := enc.Encode(proto.CmdPing, 1, nil); err != nil {
		t.Fatalf("send ping: %v", err)
	}
	if err := enc.Flush(); err != nil {
		t.Fatalf("flush ping: %v", err)
	}

	_ = tcpConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	dec := proto.NewDecoder(tcpConn)
	f, err := dec.Decode()
	if err != nil {
		logs := dockerLogs(t, containerName)
		t.Fatalf("read pong from TCP %s failed: %v\n\nContainer logs:\n%s", tcpAddr, err, logs)
	}
	if f.Command != proto.CmdPong {
		t.Fatalf("TCP port %s: expected CmdPong, got %s", tcpAddr, f.Command)
	}

	t.Logf("Docker smoke test passed: TCP ping/pong OK on port %d", tcpPort)
}

func TestDockerSmokeRejectsPortDrift(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not on PATH; skipping smoke test")
	}

	absRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}

	tmpDir := t.TempDir()

	badConfig := `{
  "broker": {"node_id": "drift-test"},
  "network": {"port": 9001, "host": "0.0.0.0", "max_connections": 10000,
    "read_timeout": 30000000000, "write_timeout": 30000000000,
    "idle_timeout": 300000000000, "dashboard_enabled": true},
  "storage": {"wal_path": "./data/wal", "data_path": "./data/segments",
    "segment_max_bytes": 1073741824, "index_interval_bytes": 4096, "sync_policy": "interval"},
  "replication": {"factor": 1, "sync_interval": 100000000, "ack_timeout": 5000000000},
  "retention": {"max_age_hours": 24, "max_size_mb": 1024},
  "auth": {"enabled": false},
  "rate_limit": {"enabled": false, "per_client_rps": 10000, "per_topic_rps": 50000, "burst_multiplier": 2},
  "logging": {"level": "info", "format": "json"},
  "cluster": {"enabled": false},
  "compaction": {"interval_ms": 60000, "tombstone_grace_ms": 86400000},
  "gateway": {"enabled": false}
}`
	badConfigPath := filepath.Join(tmpDir, "broker.json")
	if err := os.WriteFile(badConfigPath, []byte(badConfig), 0o644); err != nil {
		t.Fatalf("write bad config: %v", err)
	}

	driftTag := "pubsub-broker-drift-test"
	driftContainer := "pubsub-broker-drift-test"

	t.Cleanup(func() {
		exec.Command("docker", "stop", "-t", "2", driftContainer).Run()
		exec.Command("docker", "rm", "-f", driftContainer).Run()
	})

	buildCtx := filepath.Join(tmpDir, "build")
	if err := os.MkdirAll(buildCtx, 0o755); err != nil {
		t.Fatalf("mkdir build ctx: %v", err)
	}

	// Copy Dockerfile into build context
	dockerfileSrc := filepath.Join(absRoot, "Dockerfile")
	dockerfileDst := filepath.Join(buildCtx, "Dockerfile")
	copyFile(t, dockerfileSrc, dockerfileDst)

	// Copy the full source tree into the build context (we need it for go build)
	copyDir(t, absRoot, buildCtx)

	// Overwrite configs/broker.json with the bad config
	badConfigsDir := filepath.Join(buildCtx, "configs")
	if err := os.MkdirAll(badConfigsDir, 0o755); err != nil {
		t.Fatalf("mkdir configs: %v", err)
	}
	if err := copyFile(t, badConfigPath, filepath.Join(badConfigsDir, "broker.json")); err != nil {
		t.Fatalf("copy bad config: %v", err)
	}

	build := exec.Command("docker", "build", "-t", driftTag, ".")
	build.Dir = buildCtx
	build.Env = append(os.Environ(), "DOCKER_BUILDKIT=0")
	out, err := build.CombinedOutput()
	if err != nil {
		t.Fatalf("docker build with bad config failed:\n%s", out)
	}

	httpPort := pickPort(t)
	cmd := exec.Command("docker", "run", "-d",
		"--name", driftContainer,
		"-p", fmt.Sprintf("%d:9001", httpPort),
		"-e", "BROKER_DATA_DIR=/data",
		driftTag,
	)
	out, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker run with bad config failed:\n%s", out)
	}

	time.Sleep(5 * time.Second)

	url := fmt.Sprintf("http://127.0.0.1:%d/health", httpPort)
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatalf("expected health check to fail with port drift, but got HTTP 200")
		}
	}

	t.Logf("Docker port-drift rejection test passed: health check correctly failed with bad config")
}

func pickPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func dockerLogs(t *testing.T, name string) string {
	t.Helper()
	out, err := exec.Command("docker", "logs", name).CombinedOutput()
	if err != nil {
		return fmt.Sprintf("(could not fetch logs: %v)", err)
	}
	return string(out)
}

func copyFile(t *testing.T, src, dst string) error {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o644)
}

func copyDir(t *testing.T, src, dst string) error {
	t.Helper()
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		return copyFile(t, path, target)
	})
}
