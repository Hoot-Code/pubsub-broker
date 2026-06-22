// Command brokectl is a command-line admin tool for the pub/sub broker.
// It speaks the broker's binary protocol using pkg/client and has no knowledge
// of internal packages.
//
// Usage:
//
//	brokectl [--addr host:port] [--key apikey] [--tls] <command>
//
// Global flags:
//
//	--addr   broker address (default: 127.0.0.1:9000)
//	--key    API key for authentication (optional)
//	--tls    connect with TLS (uses system root CAs)
//
// Subcommands:
//
//	topic list
//	topic create --name <n> --partitions <p> [--retention-hours <h>]
//	topic delete --name <n>
//	consumer list
//	ping
//	publish --topic <t> --payload <s> [--key <k>]
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/Hoot-Code/pubsub-broker/pkg/client"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	// ── Global flags ─────────────────────────────────────────────────────
	fs := flag.NewFlagSet("brokectl", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addr := fs.String("addr", "127.0.0.1:9000", "broker address")
	apiKey := fs.String("key", "", "API key for authentication")
	useTLS := fs.Bool("tls", false, "connect with TLS (system root CAs)")

	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 1
	}
	remaining := fs.Args()
	if len(remaining) == 0 {
		fmt.Fprintln(os.Stderr, "usage: brokectl [--addr addr] [--key key] [--tls] <command>")
		fmt.Fprintln(os.Stderr, "commands: topic list, topic create, topic delete, consumer list, ping, publish, health, tail, consume, dlq, pprof, top")
		return 1
	}

	// ── HTTP-only commands (no TCP dial needed) ───────────────────────────
	if remaining[0] == "health" {
		return runHealth(*addr, remaining[1:])
	}
	if remaining[0] == "cluster" {
		return runCluster(*addr, remaining[1:])
	}
	if remaining[0] == "dlq" {
		return runDLQ(*addr, remaining[1:])
	}
	if remaining[0] == "pprof" {
		return runPprof(*addr, remaining[1:])
	}
	if remaining[0] == "top" {
		return runTop(*addr, remaining[1:])
	}

	// ── Dial ─────────────────────────────────────────────────────────────
	dialOpts := []client.Option{
		client.WithDialTimeout(10 * time.Second),
		client.WithReadTimeout(30 * time.Second),
	}
	if *useTLS {
		dialOpts = append(dialOpts, client.WithTLS(&tls.Config{}))
	}
	c, err := client.Dial(*addr, dialOpts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: dial %s: %v\n", *addr, err)
		return 1
	}
	defer c.Close()

	if *apiKey != "" {
		if err := c.Authenticate(*apiKey); err != nil {
			fmt.Fprintf(os.Stderr, "error: authenticate: %v\n", err)
			return 1
		}
	}

	ctx := context.Background()

	// ── Dispatch ─────────────────────────────────────────────────────────
	switch remaining[0] {
	case "topic":
		return runTopic(ctx, c, *addr, remaining[1:])
	case "consumer":
		return runConsumer(ctx, c, *addr, remaining[1:])
	case "audit":
		return runAudit(*addr, remaining[1:])
	case "ping":
		return runPing(ctx, c)
	case "publish":
		return runPublish(ctx, c, remaining[1:])
	case "tail":
		return runTail(ctx, c, remaining[1:])
	case "consume":
		return runConsume(ctx, c, remaining[1:])
	default:
		fmt.Fprintf(os.Stderr, "error: unknown command %q\n", remaining[0])
		return 1
	}
}

// ─── topic subcommands ────────────────────────────────────────────────────────

func runTopic(ctx context.Context, c *client.Client, addr string, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: topic <list|create|delete>")
		return 1
	}
	switch args[0] {
	case "list":
		return runTopicList(ctx, c)
	case "create":
		return runTopicCreate(ctx, c, args[1:])
	case "delete":
		return runTopicDelete(ctx, c, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "error: unknown topic subcommand %q\n", args[0])
		return 1
	}
}

func runTopicList(ctx context.Context, c *client.Client) int {
	topics, err := c.ListTopics(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: list topics: %v\n", err)
		return 1
	}
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tPARTITIONS\tCREATED")
	for _, t := range topics {
		created := t.CreatedAt.UTC().Format("2006-01-02 15:04:05 UTC")
		fmt.Fprintf(w, "%s\t%d\t%s\n", t.Name, t.Partitions, created)
	}
	w.Flush()
	return 0
}

func runTopicCreate(ctx context.Context, c *client.Client, args []string) int {
	fs := flag.NewFlagSet("topic create", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	name := fs.String("name", "", "topic name (required)")
	partitions := fs.Int("partitions", 1, "number of partitions")
	retentionHours := fs.Int("retention-hours", 0, "retention in hours (0 = unlimited)")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *name == "" {
		fmt.Fprintln(os.Stderr, "error: --name is required")
		return 1
	}
	err := c.CreateTopic(ctx, types.TopicConfig{
		Name:           *name,
		Partitions:     *partitions,
		RetentionHours: *retentionHours,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: create topic %q: %v\n", *name, err)
		return 1
	}
	fmt.Printf("topic %q created (%d partition(s))\n", *name, *partitions)
	return 0
}

func runTopicDelete(ctx context.Context, c *client.Client, args []string) int {
	fs := flag.NewFlagSet("topic delete", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	name := fs.String("name", "", "topic name (required)")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *name == "" {
		fmt.Fprintln(os.Stderr, "error: --name is required")
		return 1
	}
	if err := c.DeleteTopic(ctx, *name); err != nil {
		fmt.Fprintf(os.Stderr, "error: delete topic %q: %v\n", *name, err)
		return 1
	}
	fmt.Printf("topic %q deleted\n", *name)
	return 0
}

// ─── consumer list / seek / reset ────────────────────────────────────────────

// runConsumer handles the "consumer" subcommand family.
func runConsumer(ctx context.Context, c *client.Client, brokerAddr string, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: consumer <list|seek|reset>")
		return 1
	}
	switch args[0] {
	case "list":
		return runConsumerList(brokerAddr, args[1:])
	case "seek":
		return runConsumerSeek(ctx, c, args[1:])
	case "reset":
		return runConsumerReset(ctx, c, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "error: unknown consumer subcommand %q\n", args[0])
		return 1
	}
}

// runConsumerList handles "consumer list" by querying the HTTP admin endpoint.
func runConsumerList(brokerAddr string, _ []string) int {
	host, portStr, err := splitHostPort(brokerAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: parse addr %q: %v\n", brokerAddr, err)
		return 1
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: parse port %q: %v\n", portStr, err)
		return 1
	}
	httpAddr := fmt.Sprintf("http://%s:%d/consumers", host, port+1)

	resp, err := http.Get(httpAddr) //nolint:noctx
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: GET %s: %v\n", httpAddr, err)
		return 1
	}
	defer resp.Body.Close()

	var payload struct {
		Groups []struct {
			Group     string `json:"group"`
			Topic     string `json:"topic"`
			Partition int32  `json:"partition"`
			Lag       int64  `json:"lag"`
		} `json:"groups"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		fmt.Fprintf(os.Stderr, "error: decode response: %v\n", err)
		return 1
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "GROUP\tTOPIC\tPARTITION\tLAG")
	for _, g := range payload.Groups {
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\n", g.Group, g.Topic, g.Partition, g.Lag)
	}
	w.Flush()
	return 0
}

// runConsumerSeek handles "consumer seek" (C7).
//
//	brokectl consumer seek --group <g> --topic <t> [--timestamp <rfc3339>] [--end]
func runConsumerSeek(ctx context.Context, c *client.Client, args []string) int {
	fs := flag.NewFlagSet("consumer seek", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	group := fs.String("group", "", "consumer group name (required)")
	topic := fs.String("topic", "", "topic name (required)")
	tsStr := fs.String("timestamp", "", "seek to this RFC3339 timestamp")
	toEnd := fs.Bool("end", false, "seek to latest offset")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *group == "" || *topic == "" {
		fmt.Fprintln(os.Stderr, "error: --group and --topic are required")
		return 1
	}

	cs := c.NewConsumer(*group, *topic)
	var (
		offsets map[int32]int64
		err     error
	)
	switch {
	case *toEnd:
		offsets, err = cs.SeekToEnd(ctx)
	case *tsStr != "":
		t, tErr := time.Parse(time.RFC3339, *tsStr)
		if tErr != nil {
			fmt.Fprintf(os.Stderr, "error: invalid timestamp %q: %v\n", *tsStr, tErr)
			return 1
		}
		offsets, err = cs.SeekToTimestamp(ctx, t.UnixNano())
	default:
		offsets, err = cs.SeekToTimestamp(ctx, 0)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: seek: %v\n", err)
		return 1
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "PARTITION\tOFFSET")
	parts := make([]int, 0, len(offsets))
	for p := range offsets {
		parts = append(parts, int(p))
	}
	sort.Ints(parts)
	for _, p := range parts {
		fmt.Fprintf(w, "%d\t%d\n", p, offsets[int32(p)])
	}
	w.Flush()
	return 0
}

// runConsumerReset handles "consumer reset" (C7).
//
//	brokectl consumer reset --group <g> --topic <t>
func runConsumerReset(ctx context.Context, c *client.Client, args []string) int {
	fs := flag.NewFlagSet("consumer reset", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	group := fs.String("group", "", "consumer group name (required)")
	topic := fs.String("topic", "", "topic name (required)")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *group == "" || *topic == "" {
		fmt.Fprintln(os.Stderr, "error: --group and --topic are required")
		return 1
	}

	cs := c.NewConsumer(*group, *topic)
	if err := cs.Reset(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "error: reset: %v\n", err)
		return 1
	}
	fmt.Printf("group %s on topic %s reset to offset 0\n", *group, *topic)
	return 0
}

// ─── ping ─────────────────────────────────────────────────────────────────────

func runPing(ctx context.Context, c *client.Client) int {
	result, err := c.Ping(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: ping: %v\n", err)
		return 1
	}
	rttMs := float64(result.RTT.Microseconds()) / 1000.0
	fmt.Printf("PONG %.2fms\n", rttMs)
	return 0
}

// ─── publish ──────────────────────────────────────────────────────────────────

func runPublish(ctx context.Context, c *client.Client, args []string) int {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	topic := fs.String("topic", "", "topic name (required)")
	payload := fs.String("payload", "", "message payload string (required)")
	key := fs.String("key", "", "routing key (optional)")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *topic == "" {
		fmt.Fprintln(os.Stderr, "error: --topic is required")
		return 1
	}
	if *payload == "" {
		fmt.Fprintln(os.Stderr, "error: --payload is required")
		return 1
	}

	prod := c.NewProducer(*topic)
	offset, err := prod.Publish(ctx, *key, []byte(*payload), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: publish: %v\n", err)
		return 1
	}
	fmt.Printf("offset=%d\n", offset)
	return 0
}

// ─── health ───────────────────────────────────────────────────────────────────

// runHealth implements "brokectl health [--addr host:port]" (B4).
// Calls GET /healthz/ready on the broker's HTTP admin port and prints
// READY (exit 0) or NOT READY: <reason> (exit 1).
func runHealth(brokerAddr string, args []string) int {
	fs := flag.NewFlagSet("health", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addr := fs.String("addr", brokerAddr, "broker address (host:port)")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	host, portStr, err := splitHostPort(*addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: parse addr %q: %v\n", *addr, err)
		return 1
	}
	port := 9000
	if p, convErr := strconv.Atoi(portStr); convErr == nil {
		port = p
	}
	httpAddr := fmt.Sprintf("http://%s:%d/healthz/ready", host, port+1)

	resp, err := http.Get(httpAddr) //nolint:noctx
	if err != nil {
		fmt.Fprintf(os.Stderr, "NOT READY: %v\n", err)
		return 1
	}
	defer resp.Body.Close()

	var payload map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		fmt.Fprintf(os.Stderr, "NOT READY: decode error: %v\n", err)
		return 1
	}

	if resp.StatusCode == http.StatusOK {
		fmt.Println("READY")
		return 0
	}
	reason, _ := payload["reason"].(string)
	if reason == "" {
		reason, _ = payload["status"].(string)
	}
	fmt.Fprintf(os.Stderr, "NOT READY: %s\n", reason)
	return 1
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// splitHostPort splits an address like "host:port" handling IPv6 addresses.
func splitHostPort(addr string) (host, port string, err error) {
	// net.SplitHostPort handles [::1]:port style addresses.
	if strings.Contains(addr, ":") {
		lastColon := strings.LastIndex(addr, ":")
		// Check for IPv6 bracket notation.
		if strings.Contains(addr, "[") {
			// Let the stdlib handle it.
			return splitHostPortStdlib(addr)
		}
		return addr[:lastColon], addr[lastColon+1:], nil
	}
	return addr, "9000", nil
}

func splitHostPortStdlib(addr string) (host, port string, err error) {
	// Minimal IPv6 split: [host]:port
	if len(addr) > 0 && addr[0] == '[' {
		close := strings.Index(addr, "]")
		if close < 0 {
			return "", "", fmt.Errorf("missing ']' in address %q", addr)
		}
		host = addr[1:close]
		rest := addr[close+1:]
		if len(rest) == 0 {
			return host, "", nil
		}
		if rest[0] != ':' {
			return "", "", fmt.Errorf("missing port in address %q", addr)
		}
		return host, rest[1:], nil
	}
	return splitHostPort(addr)
}

// ─── cluster subcommands ──────────────────────────────────────────────────────

// runCluster dispatches "brokectl cluster <subcommand>".
// Currently supports: isr
func runCluster(addr string, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: cluster <isr>")
		return 1
	}
	switch args[0] {
	case "isr":
		return runClusterISR(addr)
	default:
		fmt.Fprintf(os.Stderr, "error: unknown cluster subcommand %q\n", args[0])
		return 1
	}
}

// runClusterISR prints a tab-separated ISR summary for every registered
// partition to stdout.
//
//	TOPIC    PARTITION  ISR_SIZE  LEADER    UNDER_REPLICATED
//	orders   0          2         node-b    false
//	orders   1          1         node-b    true
func runClusterISR(addr string) int {
	host, portStr, err := splitHostPort(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: parse addr %q: %v\n", addr, err)
		return 1
	}
	port := 9000
	if p, convErr := strconv.Atoi(portStr); convErr == nil {
		port = p
	}

	// The HTTP admin port is broker TCP port + 1.
	httpAddr := fmt.Sprintf("http://%s:%d/cluster/isr", host, port+1)
	resp, err := http.Get(httpAddr) //nolint:noctx
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: GET %s: %v\n", httpAddr, err)
		return 1
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "error: HTTP %d from %s\n", resp.StatusCode, httpAddr)
		return 1
	}

	type isrEntry struct {
		Topic           string   `json:"topic"`
		Partition       int32    `json:"partition"`
		ISR             []string `json:"isr"`
		Leader          string   `json:"leader"`
		UnderReplicated bool     `json:"under_replicated"`
	}
	var entries []isrEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		fmt.Fprintf(os.Stderr, "error: decode response: %v\n", err)
		return 1
	}

	// Print tab-separated table.
	fmt.Fprintf(os.Stdout, "TOPIC\tPARTITION\tISR_SIZE\tLEADER\tUNDER_REPLICATED\n")
	for _, e := range entries {
		fmt.Fprintf(os.Stdout, "%s\t%d\t%d\t%s\t%v\n",
			e.Topic, e.Partition, len(e.ISR), e.Leader, e.UnderReplicated)
	}
	return 0
}

// ─── audit subcommands ────────────────────────────────────────────────────────

// runAudit dispatches "audit" subcommands.
//
// Usage:
//
//	brokectl audit tail [--n <count>]
func runAudit(addr string, args []string) int {
	if len(args) == 0 {
		return runAuditTail(addr, nil)
	}
	if args[0] == "tail" {
		return runAuditTail(addr, args[1:])
	}
	fmt.Fprintf(os.Stderr, "error: unknown audit subcommand %q\n", args[0])
	return 1
}

// runAuditTail calls GET /audit/recent and prints the last N events in a
// human-readable table.
//
//	TIME                     TYPE           CLIENT          TOPIC    OK
//	2024-01-15 10:23:01      publish        svc-orders      orders   true
//	2024-01-15 10:23:00      auth           svc-orders               true
func runAuditTail(addr string, args []string) int {
	fs := flag.NewFlagSet("audit tail", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	n := fs.Int("n", 20, "number of recent events to show")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 1
	}

	host, portStr, err := splitHostPort(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: parse addr %q: %v\n", addr, err)
		return 1
	}
	port := 9000
	if p, convErr := strconv.Atoi(portStr); convErr == nil {
		port = p
	}

	httpAddr := fmt.Sprintf("http://%s:%d/audit/recent?n=%d", host, port+1, *n)
	resp, err := http.Get(httpAddr) //nolint:noctx
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: GET %s: %v\n", httpAddr, err)
		return 1
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "error: HTTP %d from %s\n", resp.StatusCode, httpAddr)
		return 1
	}

	type auditEvent struct {
		Time       time.Time         `json:"time"`
		Type       string            `json:"type"`
		ClientID   string            `json:"client_id"`
		RemoteAddr string            `json:"remote_addr"`
		Topic      string            `json:"topic,omitempty"`
		Success    bool              `json:"success"`
		Error      string            `json:"error,omitempty"`
		Details    map[string]string `json:"details,omitempty"`
	}

	var events []auditEvent
	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		fmt.Fprintf(os.Stderr, "error: decode response: %v\n", err)
		return 1
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "TIME\tTYPE\tCLIENT\tTOPIC\tOK")
	for _, e := range events {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%v\n",
			e.Time.Format("2006-01-02 15:04:05"),
			e.Type,
			e.ClientID,
			e.Topic,
			e.Success,
		)
	}
	tw.Flush()
	return 0
}

// ─── tail subcommand ──────────────────────────────────────────────────────────

// runTail implements "brokectl tail" — live message streaming.
func runTail(ctx context.Context, c *client.Client, args []string) int {
	fs := flag.NewFlagSet("tail", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	topic := fs.String("topic", "", "topic name (required)")
	group := fs.String("group", "", "consumer group (default: brokectl-tail)")
	follow := fs.Bool("follow", false, "follow new messages")
	offset := fs.Int64("offset", -1, "start offset (-1 = latest)")
	count := fs.Int64("count", 0, "max messages to print (0 = unlimited)")
	partition := fs.Int("partition", -1, "specific partition (-1 = all)")
	format := fs.String("format", "text", "output format: text or json")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *topic == "" {
		fmt.Fprintln(os.Stderr, "error: --topic is required")
		return 1
	}
	if *group == "" {
		*group = "brokectl-tail"
	}

	cs := c.NewConsumer(*group, *topic)
	if err := cs.Subscribe(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "error: subscribe: %v\n", err)
		return 1
	}
	defer cs.Close()

	if *offset >= 0 {
		if _, err := cs.SeekToOffset(ctx, *offset); err != nil {
			fmt.Fprintf(os.Stderr, "error: seek: %v\n", err)
			return 1
		}
	}

	printed := int64(0)
	for {
		select {
		case <-ctx.Done():
			return 0
		case msg, ok := <-cs.Messages():
			if !ok {
				return 0
			}
			if *partition >= 0 && msg.Partition != int32(*partition) {
				continue
			}
			if *format == "json" {
				line, _ := json.Marshal(map[string]interface{}{
					"partition": msg.Partition,
					"offset":    msg.Offset,
					"key":       msg.Key,
					"ts":        time.Unix(0, msg.Timestamp).Format(time.RFC3339),
					"size":      len(msg.Payload),
					"payload":   string(msg.Payload),
				})
				fmt.Println(string(line))
			} else {
				ts := time.Unix(0, msg.Timestamp).Format(time.RFC3339)
				payload := truncatePayload(msg.Payload, 120)
				fmt.Printf("[P%d:O%d] key=%s ts=%s size=%dB\n%s\n",
					msg.Partition, msg.Offset, msg.Key, ts, len(msg.Payload), payload)
			}
			printed++
			if *count > 0 && printed >= *count {
				return 0
			}
		case <-time.After(2 * time.Second):
			if !*follow && printed > 0 {
				return 0
			}
			if !*follow && printed == 0 {
				return 0
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
}

// truncatePayload truncates payload to maxLen chars, replacing non-printable bytes with '.'.
func truncatePayload(b []byte, maxLen int) string {
	if len(b) > maxLen {
		b = b[:maxLen]
	}
	out := make([]byte, len(b))
	for i, c := range b {
		if c >= 32 && c < 127 {
			out[i] = c
		} else {
			out[i] = '.'
		}
	}
	return string(out)
}

// ─── consume subcommand ───────────────────────────────────────────────────────

// runConsume implements "brokectl consume" — single-batch interactive fetch.
func runConsume(ctx context.Context, c *client.Client, args []string) int {
	fs := flag.NewFlagSet("consume", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	topic := fs.String("topic", "", "topic name (required)")
	group := fs.String("group", "", "consumer group (required)")
	consumerID := fs.String("consumer", "", "consumer ID (default: brokectl-<hostname>)")
	partition := fs.Int("partition", -1, "specific partition (-1 = all)")
	batch := fs.Int("batch", 10, "messages per fetch call")
	commit := fs.Bool("commit", false, "auto-commit offset after each batch")
	format := fs.String("format", "text", "output format: text or json")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *topic == "" {
		fmt.Fprintln(os.Stderr, "error: --topic is required")
		return 1
	}
	if *group == "" {
		fmt.Fprintln(os.Stderr, "error: --group is required")
		return 1
	}
	if *consumerID == "" {
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "brokectl"
		}
		*consumerID = "brokectl-" + hostname
	}

	cs := c.NewConsumer(*group, *topic)
	if err := cs.Subscribe(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "error: subscribe: %v\n", err)
		return 1
	}
	defer cs.Close()

	received := 0
	var maxOffset int64 = -1
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

loop:
	for received < *batch {
		select {
		case <-ctx.Done():
			break loop
		case <-timer.C:
			if received == 0 {
				fmt.Fprintln(os.Stderr, "no messages available")
			}
			break loop
		case msg, ok := <-cs.Messages():
			if !ok {
				break loop
			}
			if *partition >= 0 && msg.Partition != int32(*partition) {
				continue
			}
			if *format == "json" {
				line, _ := json.Marshal(map[string]interface{}{
					"partition": msg.Partition,
					"offset":    msg.Offset,
					"key":       msg.Key,
					"ts":        time.Unix(0, msg.Timestamp).Format(time.RFC3339),
					"size":      len(msg.Payload),
					"payload":   string(msg.Payload),
				})
				fmt.Println(string(line))
			} else {
				ts := time.Unix(0, msg.Timestamp).Format(time.RFC3339)
				payload := truncatePayload(msg.Payload, 120)
				fmt.Printf("[P%d:O%d] key=%s ts=%s size=%dB\n%s\n",
					msg.Partition, msg.Offset, msg.Key, ts, len(msg.Payload), payload)
			}
			if msg.Offset > maxOffset {
				maxOffset = msg.Offset
			}
			received++
		}
	}
	if *commit && maxOffset >= 0 {
		if err := cs.Commit(ctx, 0, maxOffset); err != nil {
			fmt.Fprintf(os.Stderr, "error: commit: %v\n", err)
			return 1
		}
	}
	return 0
}

// ─── dlq subcommand ───────────────────────────────────────────────────────────

// runDLQ dispatches "brokectl dlq <subcommand>".
func runDLQ(addr string, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: dlq <list|replay|purge>")
		return 1
	}
	switch args[0] {
	case "list":
		return runDLQList(addr, args[1:])
	case "replay":
		return runDLQReplay(addr, args[1:])
	case "purge":
		return runDLQPurge(addr, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "error: unknown dlq subcommand %q\n", args[0])
		return 1
	}
}

// dlqHTTPAddr constructs the HTTP admin URL for DLQ endpoints.
func dlqHTTPAddr(brokerAddr, path string) string {
	host, portStr, err := splitHostPort(brokerAddr)
	if err != nil {
		return ""
	}
	port := 9000
	if p, convErr := strconv.Atoi(portStr); convErr == nil {
		port = p
	}
	return fmt.Sprintf("http://%s:%d%s", host, port+1, path)
}

// runDLQList implements "brokectl dlq list --group <g> --topic <t> [--limit <n>]".
func runDLQList(addr string, args []string) int {
	fs := flag.NewFlagSet("dlq list", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	group := fs.String("group", "", "consumer group (required)")
	topic := fs.String("topic", "", "topic name (required)")
	limit := fs.Int("limit", 100, "max entries to show")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *group == "" || *topic == "" {
		fmt.Fprintln(os.Stderr, "error: --group and --topic are required")
		return 1
	}
	url := fmt.Sprintf("%s?group=%s&topic=%s&limit=%d",
		dlqHTTPAddr(addr, "/dlq"), *group, *topic, *limit)
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: GET %s: %v\n", url, err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		fmt.Fprintln(os.Stderr, "no DLQ entries found")
		return 0
	}
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "error: HTTP %d\n", resp.StatusCode)
		return 1
	}
	var entries []struct {
		ID         string `json:"id"`
		Topic      string `json:"topic"`
		Partition  int32  `json:"partition"`
		Offset     int64  `json:"offset"`
		Key        string `json:"key"`
		Attempts   int    `json:"attempts"`
		EnqueuedAt string `json:"enqueued_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		fmt.Fprintf(os.Stderr, "error: decode: %v\n", err)
		return 1
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tPARTITION\tOFFSET\tATTEMPTS\tENQUEUED_AT\tKEY")
	for _, e := range entries {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%s\t%s\n",
			e.ID, e.Partition, e.Offset, e.Attempts, e.EnqueuedAt, e.Key)
	}
	tw.Flush()
	return 0
}

// runDLQReplay implements "brokectl dlq replay --group <g> --topic <t> [--limit <n>]".
func runDLQReplay(addr string, args []string) int {
	fs := flag.NewFlagSet("dlq replay", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	group := fs.String("group", "", "consumer group (required)")
	topic := fs.String("topic", "", "topic name (required)")
	limit := fs.Int("limit", 10, "max entries to replay")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *group == "" || *topic == "" {
		fmt.Fprintln(os.Stderr, "error: --group and --topic are required")
		return 1
	}
	url := fmt.Sprintf("%s?group=%s&topic=%s&limit=%d",
		dlqHTTPAddr(addr, "/dlq/replay"), *group, *topic, *limit)
	resp, err := http.Post(url, "application/json", nil) //nolint:noctx
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: POST %s: %v\n", url, err)
		return 1
	}
	defer resp.Body.Close()
	var result struct {
		Replayed  int `json:"replayed"`
		Remaining int `json:"remaining"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Fprintf(os.Stderr, "error: decode: %v\n", err)
		return 1
	}
	fmt.Printf("replayed: %d, remaining: %d\n", result.Replayed, result.Remaining)
	return 0
}

// runDLQPurge implements "brokectl dlq purge --group <g> --topic <t>".
func runDLQPurge(addr string, args []string) int {
	fs := flag.NewFlagSet("dlq purge", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	group := fs.String("group", "", "consumer group (required)")
	topic := fs.String("topic", "", "topic name (required)")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *group == "" || *topic == "" {
		fmt.Fprintln(os.Stderr, "error: --group and --topic are required")
		return 1
	}
	req, err := http.NewRequest(http.MethodDelete,
		fmt.Sprintf("%s?group=%s&topic=%s",
			dlqHTTPAddr(addr, "/dlq"), *group, *topic), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: create request: %v\n", err)
		return 1
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: DELETE: %v\n", err)
		return 1
	}
	defer resp.Body.Close()
	var result struct {
		Purged int `json:"purged"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Fprintf(os.Stderr, "error: decode: %v\n", err)
		return 1
	}
	fmt.Printf("purged: %d\n", result.Purged)
	return 0
}

// ─── pprof subcommand ─────────────────────────────────────────────────────────

// runPprof implements "brokectl pprof" — download pprof profiles.
func runPprof(addr string, args []string) int {
	fs := flag.NewFlagSet("pprof", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	profType := fs.String("type", "", "profile type: cpu, heap, goroutine, trace (required)")
	seconds := fs.Int("seconds", 30, "duration for cpu/trace profiles")
	output := fs.String("output", "", "output file (default: <type>.prof)")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *profType == "" {
		fmt.Fprintln(os.Stderr, "error: --type is required")
		return 1
	}
	switch *profType {
	case "cpu", "heap", "goroutine", "trace":
	default:
		fmt.Fprintf(os.Stderr, "error: unknown profile type %q (expected cpu, heap, goroutine, trace)\n", *profType)
		return 1
	}
	if *output == "" {
		*output = *profType + ".prof"
	}

	var url string
	switch *profType {
	case "cpu":
		url = fmt.Sprintf("%s?seconds=%d", dlqHTTPAddr(addr, "/debug/pprof/profile"), *seconds)
	case "heap":
		url = dlqHTTPAddr(addr, "/debug/pprof/heap")
	case "goroutine":
		url = dlqHTTPAddr(addr, "/debug/pprof/goroutine")
	case "trace":
		url = fmt.Sprintf("%s?seconds=%d", dlqHTTPAddr(addr, "/debug/pprof/trace"), *seconds)
	}
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: GET %s: %v\n", url, err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "error: HTTP %d from %s\n", resp.StatusCode, url)
		return 1
	}
	f, err := os.Create(*output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: create %s: %v\n", *output, err)
		return 1
	}
	defer f.Close()
	if _, err := io.Copy(f, resp.Body); err != nil {
		fmt.Fprintf(os.Stderr, "error: write profile: %v\n", err)
		return 1
	}
	fmt.Printf("profile saved to %s\n", *output)
	return 0
}

// ─── top subcommand ───────────────────────────────────────────────────────────

// runTop implements "brokectl top" — live metrics dashboard.
func runTop(addr string, args []string) int {
	fs := flag.NewFlagSet("top", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	intervalMs := fs.Int("interval", 1000, "poll interval in milliseconds")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if *intervalMs <= 0 {
		*intervalMs = 1000
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	host, portStr, err := splitHostPort(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: parse addr %q: %v\n", addr, err)
		return 1
	}
	port := 9000
	if p, convErr := strconv.Atoi(portStr); convErr == nil {
		port = p
	}
	metricsURL := fmt.Sprintf("http://%s:%d/metrics", host, port+1)
	healthURL := fmt.Sprintf("http://%s:%d/health", host, port+1)

	var prevPublished, prevConsumed float64
	startTime := time.Now()
	firstPoll := true

	ticker := time.NewTicker(time.Duration(*intervalMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-sigCh:
			fmt.Println()
			return 0
		case <-ticker.C:
			resp, err := http.Get(metricsURL) //nolint:noctx
			if err != nil {
				fmt.Fprintf(os.Stderr, "\rerror: GET %s: %v\n", metricsURL, err)
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			metrics := parsePrometheusMetrics(body)

			var nodeID string
			hresp, herr := http.Get(healthURL) //nolint:noctx
			if herr == nil {
				var hp map[string]string
				json.NewDecoder(hresp.Body).Decode(&hp)
				hresp.Body.Close()
				nodeID = hp["node_id"]
			}

			uptime := time.Since(startTime)
			d := int(uptime.Hours()) / 24
			h := int(uptime.Hours()) % 24
			m := int(uptime.Minutes()) % 60
			uptimeStr := fmt.Sprintf("%dd %dh %dm", d, h, m)

			pubTotal := metrics["pubsub_messages_published_total"]
			conTotal := metrics["pubsub_messages_consumed_total"]
			pubRate := pubTotal - prevPublished
			conRate := conTotal - prevConsumed
			if firstPoll {
				pubRate = 0
				conRate = 0
				firstPoll = false
			}
			prevPublished = pubTotal
			prevConsumed = conTotal

			fmt.Print("\033[2J\033[H")
			fmt.Printf("pubsub-broker  node=%s  uptime=%s\n", nodeID, uptimeStr)
			fmt.Println("─────────────────────────────────────────────")
			fmt.Printf("messages/sec (publish):  %.0f\n", pubRate)
			fmt.Printf("messages/sec (consume):  %.0f\n", conRate)
			fmt.Printf("consumer lag (total):    %.0f\n", metrics["pubsub_consumer_lag_total"])
			fmt.Printf("active connections:      %.0f\n", metrics["pubsub_active_connections"])
			fmt.Printf("WAL bytes total:         %.0f MB\n", metrics["pubsub_wal_bytes_total"]/1024/1024)
			fmt.Printf("topics:                  %.0f   partitions: %.0f\n",
				metrics["pubsub_topic_count"], metrics["pubsub_partition_count"])
			fmt.Printf("ISR size (avg):          %.0f   under-replicated: %.0f\n",
				metrics["pubsub_cluster_isr_size"], metrics["pubsub_cluster_under_replicated_partitions"])
		}
	}
}

// parsePrometheusMetrics parses Prometheus text exposition format into a map.
func parsePrometheusMetrics(data []byte) map[string]float64 {
	result := make(map[string]float64)
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			continue
		}
		name := parts[0]
		if idx := strings.Index(name, "{"); idx >= 0 {
			name = name[:idx]
		}
		val, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}
		result[name] = val
	}
	return result
}
