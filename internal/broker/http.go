package broker

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	netpprof "net/http/pprof"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
)

//go:embed assets/dashboard/*
var dashboardFS embed.FS

//go:embed assets/login.html
var loginHTML []byte

// buildHTTPServer creates the HTTP admin/metrics server, wiring all endpoints.
func (b *Broker) buildHTTPServer(cfg *config.Config) *http.Server {
	mux := http.NewServeMux()

	// ── Endpoints that NEVER require auth ──────────────────────────────────
	// /health, /status — basic health checks.
	mux.HandleFunc("/health", b.httpHealth)
	mux.HandleFunc("/status", b.httpHealth)
	// /metrics — Prometheus scrape; scrapers cannot do interactive login.
	mux.HandleFunc("/metrics", b.httpMetrics)
	// /healthz/* — Kubernetes probes.
	mux.HandleFunc("/healthz/live", b.httpHealthLive)
	mux.HandleFunc("/healthz/ready", b.httpHealthReady)
	mux.HandleFunc("/healthz/startup", b.httpHealthStartup)
	mux.HandleFunc("/healthz/drain", b.httpHealthDrain)
	// Dashboard auth endpoints — they ARE the auth mechanism.
	// /dashboard/session is always registered so the frontend can check auth state.
	if cfg.Network.DashboardEnabled && cfg.Auth.Enabled {
		mux.HandleFunc("POST /dashboard/login", b.httpDashboardLogin)
		mux.HandleFunc("POST /dashboard/logout", b.httpDashboardLogout)
		mux.HandleFunc("/dashboard/login", b.httpDashboardLogin)
	}
	if cfg.Network.DashboardEnabled {
		mux.HandleFunc("GET /dashboard/session", b.httpDashboardSession)
	}

	// ── Endpoints that require auth when DashboardAuthEnabled is true ─────
	// Wrap each with requireAuth; when auth is off the middleware is a no-op.
	authWrap := b.requireAuth

	// API endpoints — accessible via Authorization header or session cookie.
	mux.HandleFunc("/topics", authWrap(b.httpTopics))
	mux.HandleFunc("/consumers", authWrap(b.httpConsumers))
	mux.HandleFunc("/traces", authWrap(b.httpTraces))
	mux.HandleFunc("/cluster/members", authWrap(b.httpClusterMembers))
	mux.HandleFunc("/cluster/partitions", authWrap(b.httpClusterPartitions))
	mux.HandleFunc("/cluster/isr", authWrap(b.httpClusterISR))
	// Audit endpoint.
	mux.HandleFunc("/audit/recent", authWrap(b.httpAuditRecent))
	// DLQ endpoints.
	mux.HandleFunc("/dlq", authWrap(b.httpDLQ))
	mux.HandleFunc("POST /dlq/replay", authWrap(b.httpDLQReplay))
	mux.HandleFunc("DELETE /dlq/{id}", authWrap(b.httpDLQDelete))
	mux.HandleFunc("GET /dlq/{id}/export", authWrap(b.httpDLQExport))
	// Partition detail endpoints.
	mux.HandleFunc("GET /topics/{topic}/partitions/{partition}", authWrap(b.httpPartitionDetail))
	mux.HandleFunc("GET /topics/{topic}/partitions", authWrap(b.httpPartitionList))
	// Consumer group detail endpoint.
	mux.HandleFunc("GET /consumers/{group}/{topic}", authWrap(b.httpConsumerGroupDetail))
	// Raft internals endpoint.
	mux.HandleFunc("GET /cluster/raft", authWrap(b.httpClusterRaft))
	// Metrics history endpoint.
	mux.HandleFunc("GET /metrics/history", authWrap(b.httpMetricsHistory))
	// Live Message Explorer WebSocket endpoint.
	mux.HandleFunc("GET /explorer/stream", authWrap(b.httpExplorerStream))
	// Config effective endpoint (read-only, admin-only).
	mux.HandleFunc("GET /config/effective", authWrap(b.httpConfigEffective))
	// Config patch endpoint (write, admin-only).
	mux.HandleFunc("PATCH /config", authWrap(b.httpConfigPatch))

	// pprof profiling endpoints.
	if cfg.Network.PprofEnabled {
		mux.HandleFunc("/debug/pprof/", netpprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", netpprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", netpprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", netpprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", netpprof.Trace)
	} else {
		mux.HandleFunc("/debug/pprof/", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]string{"error": "pprof is disabled"})
		})
	}

	// ── Dashboard and root ─────────────────────────────────────────────────
	if cfg.Network.DashboardEnabled {
		// Serve dashboard static assets (CSS, JS) from the embedded FS.
		dashFS, _ := fs.Sub(dashboardFS, "assets/dashboard")
		fileServer := http.StripPrefix("/dashboard/", http.FileServerFS(dashFS))
		mux.Handle("/dashboard/", authWrap(func(w http.ResponseWriter, r *http.Request) {
			fileServer.ServeHTTP(w, r)
		}))
		mux.HandleFunc("/dashboard", b.httpDashboardWithAuth)
		mux.HandleFunc("/", authWrap(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" {
				http.NotFound(w, r)
				return
			}
			http.Redirect(w, r, "/dashboard", http.StatusFound)
		}))
	} else {
		mux.HandleFunc("/dashboard", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]string{"error": "dashboard is disabled"})
		})
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/" {
				http.NotFound(w, r)
				return
			}
			http.NotFound(w, r)
		})
	}

	httpPort := cfg.Network.Port
	if httpPort > 0 {
		httpPort++
	}
	if httpPort > 65535 {
		httpPort = 65535
	}
	return &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Network.Host, httpPort),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}
}

// httpDashboardWithAuth serves the dashboard HTML or the login page.
// It is intentionally not wrapped with requireAuth so that unauthenticated
// browsers land here and see the login form rather than a JSON 401. When
// dashboard auth is enabled and no valid session exists, loginHTML is served.
// When auth is disabled or the session is valid, index.html is served.
func (b *Broker) httpDashboardWithAuth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if b.dashboardAuthEnabled() {
		// Check whether the request carries a live session cookie. If not,
		// or if the session has expired, show the login form so the user
		// can authenticate before reaching the dashboard.
		cookie, err := r.Cookie("pubsub_dashboard_session")
		if err != nil || cookie.Value == "" {
			w.Write(loginHTML)
			return
		}
		if _, ok := b.sessionStore.Get(cookie.Value); !ok {
			w.Write(loginHTML)
			return
		}
		// Valid session — fall through and serve the dashboard.
	}
	data, err := dashboardFS.ReadFile("assets/dashboard/index.html")
	if err != nil {
		http.Error(w, "dashboard not found", http.StatusInternalServerError)
		return
	}
	w.Write(data)
}

// DashboardHTML returns the embedded dashboard index.html content. Intended for testing.
func DashboardHTML() []byte {
	data, err := dashboardFS.ReadFile("assets/dashboard/index.html")
	if err != nil {
		return nil
	}
	return data
}

// LoginHTML returns the embedded login HTML content. Intended for testing.
func LoginHTML() []byte {
	return loginHTML
}
