# Contributing

Thank you for considering a contribution to pubsub-broker. The sections below cover everything you need to get from a fresh checkout to a passing pull request.

## Running Tests

```bash
# Full test suite with race detector (required before every PR)
go test -race ./...

# Single package
go test -race -v ./internal/broker/...

# Run a specific test by name
go test -race -run TestGracefulStop ./internal/broker/...
```

All tests must pass with `-race` and with zero data races. Tests that require network ports use `port: 0` (ephemeral) so they do not collide.

## Running Benchmarks

```bash
# Run all benchmarks for 5 seconds each
go test -bench=. -benchtime=5s ./tests/benchmarks/

# Run a specific benchmark
go test -bench=BenchmarkPublish -benchtime=10s -benchmem ./tests/benchmarks/
```

Benchmark results are sensitive to machine state. Run at least three iterations and discard the first to allow the OS page cache to warm up.

## Coding Standards

**No external dependencies.** The `go.mod` file must have an empty `require` block. Use only the Go standard library. This is non-negotiable — it keeps the binary self-contained and auditable.

**Error wrapping.** Always use `fmt.Errorf("context: %w", err)` — never `%v` — so callers can use `errors.Is` and `errors.As` to inspect the error chain.

**Godoc on every export.** Every exported type, function, method, and constant must have a godoc comment that starts with the symbol name. Run `go doc ./...` to spot gaps.

**`go vet` must pass.** Run `go vet ./...` before pushing. The CI gate will reject any PR that introduces a vet finding.

**`gofmt` formatting.** Run `gofmt -w .` before pushing. Diffs must be empty.

**Test coverage.** Every new exported function must have at least one test that exercises both the happy path and a representative error path.

## How to Add a New Protocol Command

Adding a command end-to-end involves five files. Use `CmdFoo = 0xNN` as a worked example below.

**Step 1 — `internal/protocol/protocol.go`**

Add the command constant and request/response structs:

```go
const CmdFoo = 0x19 // choose next available hex code

// FooRequest is the body of a CmdFoo frame.
type FooRequest struct {
    Name string `json:"name"`
}

// FooResponse is the body of a CmdFoo reply frame.
type FooResponse struct {
    Result string `json:"result"`
}
```

**Step 2 — `internal/broker/broker.go` dispatch**

In the `dispatch` switch inside `handleConn`, add a case for the new command:

```go
case protocol.CmdFoo:
    return b.handleFoo(ctx, conn, frame)
```

**Step 3 — Add the handler**

Create a new file `internal/broker/handle_foo.go`:

```go
package broker

import (
    "context"
    "encoding/json"
    "fmt"

    "github.com/Hoot-Code/pubsub-broker/internal/protocol"
    "github.com/Hoot-Code/pubsub-broker/internal/server"
)

// handleFoo handles CmdFoo requests.
func (b *Broker) handleFoo(ctx context.Context, conn server.Conn, frame *protocol.Frame) error {
    var req protocol.FooRequest
    if err := json.Unmarshal(frame.Body, &req); err != nil {
        return fmt.Errorf("handleFoo: unmarshal: %w", err)
    }
    resp := protocol.FooResponse{Result: "ok:" + req.Name}
    return conn.WriteFrame(protocol.CmdFoo, frame.RequestID, resp)
}
```

**Step 4 — Client SDK (`pkg/client/client.go`)**

Add a `Foo(name string) (string, error)` method to the `Client` struct that sends a `CmdFoo` frame and waits for the response frame.

**Step 5 — Tests**

Add a `TestFoo` function in `internal/broker/broker_foo_test.go` that starts an in-process broker, calls `client.Foo`, and asserts the response.

## Docker Smoke Test

The Docker smoke test builds the image, starts a container, and verifies the health endpoint responds with HTTP 200. This catches classes of bugs where the final image is missing config files or the binary fails to start in a clean container filesystem.

```bash
# Run the Docker smoke test (requires a local Docker daemon)
make smoke-test

# Or directly via go test
go test -tags docker_smoke -v ./tests/integration/...
```

**When to run:**
- Before any Dockerfile change
- Before tagging a release
- When adding or modifying config files that the container depends on

The test skips automatically if `docker` is not on PATH.

## Pull Request Checklist

Before marking your PR ready for review:

- [ ] `go build ./...` succeeds with zero errors
- [ ] `go vet ./...` produces zero findings
- [ ] `go test -race ./...` passes with zero races
- [ ] `gofmt -d .` produces empty output
- [ ] Every new exported symbol has a godoc comment
- [ ] All errors use `fmt.Errorf("context: %w", err)`
- [ ] No new entries in `go.mod`/`go.sum` (stdlib only)
- [ ] New behaviour is covered by at least one test
- [ ] `ARCHITECTURE.md` updated if the protocol or storage format changed
- [ ] Docker smoke test passes (`make smoke-test`) if Dockerfile or configs were changed

### Pre-Release Checklist

Before tagging a release:

- [ ] `go build ./...` succeeds with zero errors
- [ ] `go vet ./...` produces zero findings
- [ ] `go test -race ./...` passes with zero races
- [ ] `make smoke-test` passes — this is the only test that builds and runs the actual Docker image end to end; it must pass before every tag/release
