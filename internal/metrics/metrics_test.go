package metrics_test

import (
	"strings"
	"testing"

	"github.com/Hoot-Code/pubsub-broker/internal/metrics"
)

func TestCounter(t *testing.T) {
	r := metrics.NewRegistry()
	c := r.NewCounter("test_counter", "a counter", nil)
	if c.Value() != 0 {
		t.Errorf("initial value: want 0, got %d", c.Value())
	}
	c.Inc(5)
	c.Inc(3)
	if c.Value() != 8 {
		t.Errorf("after increments: want 8, got %d", c.Value())
	}
}

func TestGauge(t *testing.T) {
	r := metrics.NewRegistry()
	g := r.NewGauge("test_gauge", "a gauge", nil)
	g.Set(10.5)
	if g.Value() != 10.5 {
		t.Errorf("Set: want 10.5, got %f", g.Value())
	}
	g.Add(-2.5)
	if g.Value() != 8.0 {
		t.Errorf("Add: want 8.0, got %f", g.Value())
	}
}

func TestHistogram(t *testing.T) {
	r := metrics.NewRegistry()
	h := r.NewHistogram("test_hist", "a histogram", nil, []float64{1, 5, 10})
	h.Observe(0.5)
	h.Observe(3.0)
	h.Observe(7.0)
	h.Observe(15.0)

	var buf strings.Builder
	r.Expose(&buf)
	out := buf.String()

	if !strings.Contains(out, "test_hist_count") {
		t.Error("missing count in exposition")
	}
	if !strings.Contains(out, "test_hist_sum") {
		t.Error("missing sum in exposition")
	}
}

func TestExpose_PrometheusFormat(t *testing.T) {
	r := metrics.NewRegistry()
	_ = r.NewCounter("messages_total", "total messages", map[string]string{"topic": "orders"})
	_ = r.NewGauge("active_conns", "active connections", nil)

	var buf strings.Builder
	r.Expose(&buf)
	out := buf.String()

	if !strings.Contains(out, "# HELP messages_total") {
		t.Error("missing HELP comment")
	}
	if !strings.Contains(out, "# TYPE messages_total counter") {
		t.Error("missing TYPE comment")
	}
	if !strings.Contains(out, `topic="orders"`) {
		t.Error("missing label in output")
	}
}

func TestBrokerMetrics_Registration(t *testing.T) {
	r := metrics.NewRegistry()
	bm := metrics.NewBrokerMetrics(r)

	bm.MessagesPublished.Inc(100)
	bm.ActiveConnections.Set(42)
	bm.MessageSizeBytes.Observe(1024)

	var buf strings.Builder
	r.Expose(&buf)
	out := buf.String()

	for _, want := range []string{
		"pubsub_messages_published_total",
		"pubsub_active_connections",
		"pubsub_message_size_bytes",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing metric %s in exposition", want)
		}
	}
}

// TestExposeFormat verifies that Expose emits valid Prometheus text exposition
// for a known counter and gauge — every # HELP, # TYPE, and value line must
// be syntactically correct.
func TestExposeFormat(t *testing.T) {
	r := metrics.NewRegistry()
	c := r.NewCounter("req_count", "request count", nil)
	c.Inc(42)
	g := r.NewGauge("queue_depth", "queue depth", nil)
	g.Set(7)

	var buf strings.Builder
	r.Expose(&buf)
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")

	wantLines := []string{
		"# HELP req_count request count",
		"# TYPE req_count counter",
		"req_count_total 42",
		"# HELP queue_depth queue depth",
		"# TYPE queue_depth gauge",
		"queue_depth 7",
	}
	for _, want := range wantLines {
		found := false
		for _, l := range lines {
			if l == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing expected line %q in:\n%s", want, buf.String())
		}
	}
}

// TestLabelEscaping verifies that label values containing \, ", and newline
// are correctly escaped in the exposition output.
func TestLabelEscaping(t *testing.T) {
	r := metrics.NewRegistry()
	tricky := "back\\slash \"quoted\" line\nnewline"
	_ = r.NewGauge("escaped_metric", "escaping test",
		map[string]string{"label": tricky})

	var buf strings.Builder
	r.Expose(&buf)
	out := buf.String()

	want := `label="back\\slash \"quoted\" line\nnewline"`
	if !strings.Contains(out, want) {
		t.Errorf("label not properly escaped\nwant substring: %s\ngot: %s", want, out)
	}
}

// TestCounterSuffix verifies that counters without _total in their name have
// it appended automatically in the value line.
func TestCounterSuffix(t *testing.T) {
	r := metrics.NewRegistry()
	_ = r.NewCounter("my_events", "event counter", nil)

	var buf strings.Builder
	r.Expose(&buf)
	out := buf.String()

	if !strings.Contains(out, "my_events_total") {
		t.Errorf("counter value line should contain _total suffix; got:\n%s", out)
	}
	// HELP and TYPE lines use the base name.
	if !strings.Contains(out, "# HELP my_events") {
		t.Errorf("HELP line should use base name; got:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE my_events counter") {
		t.Errorf("TYPE line should use base name; got:\n%s", out)
	}
}

// TestHelpEscaping verifies that HELP strings containing backslash and newline
// are correctly escaped in the exposition output per the Prometheus text format spec.
func TestHelpEscaping(t *testing.T) {
	r := metrics.NewRegistry()
	// Help string contains a literal backslash and a literal newline.
	help := "path\\to\\file\nwith newline"
	_ = r.NewCounter("help_escape_metric", help, nil)

	var buf strings.Builder
	r.Expose(&buf)
	out := buf.String()

	// Backslash must be doubled; newline must become \n (literal two chars).
	wantBackslash := `path\\to\\file`
	wantNewline := `\n`

	if !strings.Contains(out, wantBackslash) {
		t.Errorf("HELP backslash not escaped correctly\nwant substring %q\ngot:\n%s",
			wantBackslash, out)
	}
	if !strings.Contains(out, wantNewline) {
		t.Errorf("HELP newline not escaped correctly\nwant substring %q\ngot:\n%s",
			wantNewline, out)
	}
	// The raw newline must NOT appear on the HELP line itself.
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "# HELP help_escape_metric") {
			if strings.ContainsRune(line, '\n') {
				t.Error("HELP line must not contain a literal newline character")
			}
			break
		}
	}
}
