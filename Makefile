BINARY   := broker
MODULE   := github.com/Hoot-Code/pubsub-broker
CMD      := ./cmd/broker
BUILD    := ./build
GOFLAGS  := -trimpath

.PHONY: all build test test-race coverage bench lint vet clean run smoke-test

all: vet test build

## build: compile the broker binary
build:
	@mkdir -p $(BUILD)
	go build $(GOFLAGS) -o $(BUILD)/$(BINARY) $(CMD)

## run: start the broker with the default config
run: build
	$(BUILD)/$(BINARY) -config configs/broker.json

## test: run all tests
test:
	go test ./... -timeout 120s -count=1

## test-race: run all tests with the race detector
test-race:
	go test -race ./... -timeout 120s -count=1

## coverage: generate HTML coverage report
coverage:
	go test -coverprofile=coverage.out ./... -timeout 120s
	go tool cover -html=coverage.out -o coverage.html
	@go tool cover -func=coverage.out | tail -1

## bench: run all benchmarks (5 s each)
bench:
	go test -bench . -benchtime=5s -benchmem ./... -run='^$$'

## vet: run go vet
vet:
	go vet ./...

## lint: run staticcheck (install: go install honnef.co/go/tools/cmd/staticcheck@latest)
lint:
	staticcheck ./...

## clean: remove build artefacts
clean:
	rm -rf $(BUILD) coverage.out coverage.html

## data-dir: create the data directories used by the default config
data-dir:
	mkdir -p data/segments

## smoke-test: build and run the Docker image, verify health endpoint
smoke-test:
	go test -tags docker_smoke -v ./tests/integration/...
