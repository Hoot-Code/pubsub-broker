package broker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/broker"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
)

// newTestBrokerWithAuth creates a broker with auth enabled and the given API key.
func newTestBrokerWithAuth(t *testing.T, apiKey, clientID, role string) *broker.Broker {
	t.Helper()
	dir := t.TempDir()
	cfgData := fmt.Sprintf(`{
		"broker":  {"node_id": "auth-test-node"},
		"network": {"port": 0, "max_connections": 100,
		            "read_timeout": 5000000000, "write_timeout": 5000000000},
		"storage": {"wal_path": %q, "data_path": %q,
		            "segment_max_bytes": 1048576, "index_interval_bytes": 512, "sync_policy": "always"},
		"auth": {
			"enabled": true,
			"api_keys": [{"key": %q, "client_id": %q, "role": %q}]
		},
		"rate_limit": {"enabled": false},
		"logging":    {"level": "error", "format": "json"}
	}`, filepath.Join(dir, "wal"), filepath.Join(dir, "data"), apiKey, clientID, role)

	cfgPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgPath, []byte(cfgData), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	loader, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	t.Cleanup(loader.Close)

	b, err := broker.New(loader)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	return b
}

// loginAndCookie performs POST /dashboard/login and returns the session cookie value.
func loginAndCookie(t *testing.T, httpAddr, apiKey string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"api_key": apiKey})
	resp, err := http.Post("http://"+httpAddr+"/dashboard/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /dashboard/login: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var e map[string]string
		json.NewDecoder(resp.Body).Decode(&e)
		t.Fatalf("login status %d: %v", resp.StatusCode, e)
	}
	for _, c := range resp.Cookies() {
		if c.Name == "pubsub_dashboard_session" {
			return c.Value
		}
	}
	t.Fatal("no session cookie in response")
	return ""
}

func TestResolveIdentityFromHeader(t *testing.T) {
	b := newTestBrokerWithAuth(t, "test-key-123", "svc-a", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	req, _ := http.NewRequest("GET", "http://"+httpAddr+"/topics", nil)
	req.Header.Set("Authorization", "Bearer test-key-123")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /topics with header: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /topics: status %d, want 200", resp.StatusCode)
	}
}

func TestResolveIdentityFromSessionCookie(t *testing.T) {
	b := newTestBrokerWithAuth(t, "cookie-key-456", "web-user", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	// Login to get session cookie.
	cookieVal := loginAndCookie(t, httpAddr, "cookie-key-456")
	if cookieVal == "" {
		t.Fatal("empty cookie value")
	}

	// Request /topics with ONLY the cookie, no Authorization header.
	client := &http.Client{
		Jar: newCookieJar("pubsub_dashboard_session", cookieVal),
	}
	resp, err := client.Get("http://" + httpAddr + "/topics")
	if err != nil {
		t.Fatalf("GET /topics with cookie: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /topics: status %d, want 200", resp.StatusCode)
	}

	// Verify the session endpoint returns correct identity.
	resp2, err := client.Get("http://" + httpAddr + "/dashboard/session")
	if err != nil {
		t.Fatalf("GET /dashboard/session: %v", err)
	}
	defer resp2.Body.Close()
	var sessInfo struct {
		ClientID string `json:"client_id"`
		Role     string `json:"role"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&sessInfo); err != nil {
		t.Fatalf("decode session: %v", err)
	}
	if sessInfo.ClientID != "web-user" {
		t.Errorf("session client_id = %q, want %q", sessInfo.ClientID, "web-user")
	}
	if sessInfo.Role != "admin" {
		t.Errorf("session role = %q, want %q", sessInfo.Role, "admin")
	}
}

func TestResolveIdentityRejectsExpiredSession(t *testing.T) {
	dir := t.TempDir()
	cfgData := fmt.Sprintf(`{
		"broker":  {"node_id": "expiry-test"},
		"network": {"port": 0, "max_connections": 100,
		            "dashboard_session_ttl": 100000000,
		            "read_timeout": 5000000000, "write_timeout": 5000000000},
		"storage": {"wal_path": %q, "data_path": %q,
		            "segment_max_bytes": 1048576, "index_interval_bytes": 512, "sync_policy": "always"},
		"auth": {
			"enabled": true,
			"api_keys": [{"key": "expiry-key", "client_id": "short", "role": "admin"}]
		},
		"rate_limit": {"enabled": false},
		"logging":    {"level": "error", "format": "json"}
	}`, filepath.Join(dir, "wal"), filepath.Join(dir, "data"))

	cfgPath := filepath.Join(dir, "config.json")
	os.WriteFile(cfgPath, []byte(cfgData), 0o644)
	loader, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	t.Cleanup(loader.Close)
	b, err := broker.New(loader)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	cookieVal := loginAndCookie(t, httpAddr, "expiry-key")

	// Wait for session to expire (100ms TTL).
	time.Sleep(150 * time.Millisecond)

	client := &http.Client{
		Jar: newCookieJar("pubsub_dashboard_session", cookieVal),
	}
	resp, err := client.Get("http://" + httpAddr + "/topics")
	if err != nil {
		t.Fatalf("GET /topics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("GET /topics with expired cookie: status %d, want 401", resp.StatusCode)
	}
}

func TestMetricsEndpointNeverRequiresAuth(t *testing.T) {
	b := newTestBrokerWithAuth(t, "metrics-key", "prom", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	// GET /metrics with NO cookie and NO Authorization header.
	resp, err := http.Get("http://" + httpAddr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics without auth: status %d, want 200", resp.StatusCode)
	}
}

func TestDashboardShowsLoginWhenUnauthenticated(t *testing.T) {
	b := newTestBrokerWithAuth(t, "redirect-key", "dash-user", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	resp, err := http.Get("http://" + httpAddr + "/dashboard")
	if err != nil {
		t.Fatalf("GET /dashboard: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /dashboard unauthenticated: status %d, want 200", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("GET /dashboard unauthenticated: Content-Type %q, want text/html", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	if len(body) == 0 {
		t.Fatal("GET /dashboard unauthenticated: empty body")
	}
}

func TestLoginWrongKeyReturns401(t *testing.T) {
	b := newTestBrokerWithAuth(t, "correct-key", "admin", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	body, _ := json.Marshal(map[string]string{"api_key": "wrong-key"})
	resp, err := http.Post("http://"+httpAddr+"/dashboard/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /dashboard/login: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("login with wrong key: status %d, want 401", resp.StatusCode)
	}
}

func TestLogoutClearsSession(t *testing.T) {
	b := newTestBrokerWithAuth(t, "logout-key", "looper", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	cookieVal := loginAndCookie(t, httpAddr, "logout-key")

	// Verify session works.
	client := &http.Client{
		Jar: newCookieJar("pubsub_dashboard_session", cookieVal),
	}
	resp, err := client.Get("http://" + httpAddr + "/topics")
	if err != nil {
		t.Fatalf("GET /topics: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /topics before logout: status %d", resp.StatusCode)
	}

	// Logout.
	req, _ := http.NewRequest("POST", "http://"+httpAddr+"/dashboard/logout", nil)
	req.Header.Set("Cookie", "pubsub_dashboard_session="+cookieVal)
	logoutResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /dashboard/logout: %v", err)
	}
	logoutResp.Body.Close()
	if logoutResp.StatusCode != http.StatusOK {
		t.Fatalf("logout status: %d, want 200", logoutResp.StatusCode)
	}

	// Verify session no longer works.
	resp2, err := client.Get("http://" + httpAddr + "/topics")
	if err != nil {
		t.Fatalf("GET /topics after logout: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Errorf("GET /topics after logout: status %d, want 401", resp2.StatusCode)
	}
}

func TestDashboardAuthDisabledServesDirectly(t *testing.T) {
	dir := t.TempDir()
	cfgData := fmt.Sprintf(`{
		"broker":  {"node_id": "noauth-test"},
		"network": {"port": 0, "max_connections": 100,
		            "dashboard_enabled": true,
		            "read_timeout": 5000000000, "write_timeout": 5000000000},
		"storage": {"wal_path": %q, "data_path": %q,
		            "segment_max_bytes": 1048576, "index_interval_bytes": 512, "sync_policy": "always"},
		"auth":       {"enabled": false},
		"rate_limit": {"enabled": false},
		"logging":    {"level": "error", "format": "json"}
	}`, filepath.Join(dir, "wal"), filepath.Join(dir, "data"))

	cfgPath := filepath.Join(dir, "config.json")
	os.WriteFile(cfgPath, []byte(cfgData), 0o644)
	loader, _ := config.Load(cfgPath)
	t.Cleanup(loader.Close)
	b, _ := broker.New(loader)
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Get("http://" + httpAddr + "/dashboard")
	if err != nil {
		t.Fatalf("GET /dashboard: %v", err)
	}
	defer resp.Body.Close()
	// Should serve dashboard directly (200), not redirect to login.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /dashboard with auth disabled: status %d, want 200", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Fatalf("Content-Type = %q, want text/html", ct)
	}
}

func TestSuccessfulLoginSetsSessionCookie(t *testing.T) {
	b := newTestBrokerWithAuth(t, "login-ok-key", "login-user", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	body, _ := json.Marshal(map[string]string{"api_key": "login-ok-key"})
	resp, err := http.Post("http://"+httpAddr+"/dashboard/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /dashboard/login: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login status: %d, want 200", resp.StatusCode)
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result["status"] != "ok" {
		t.Fatalf("login response status = %q, want %q", result["status"], "ok")
	}

	found := false
	for _, c := range resp.Cookies() {
		if c.Name == "pubsub_dashboard_session" {
			found = true
			if c.Value == "" {
				t.Error("session cookie value is empty")
			}
			if c.Path != "/" {
				t.Errorf("cookie Path = %q, want %q", c.Path, "/")
			}
			if !c.HttpOnly {
				t.Error("cookie HttpOnly should be true")
			}
			if c.MaxAge <= 0 {
				t.Errorf("cookie MaxAge = %d, want > 0", c.MaxAge)
			}
		}
	}
	if !found {
		t.Error("response missing pubsub_dashboard_session cookie")
	}
}

func TestLoginReturns401WithErrorMessage(t *testing.T) {
	b := newTestBrokerWithAuth(t, "valid-key", "admin", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	body, _ := json.Marshal(map[string]string{"api_key": "invalid-key"})
	resp, err := http.Post("http://"+httpAddr+"/dashboard/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /dashboard/login: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("login status: %d, want 401", resp.StatusCode)
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result["error"] != "invalid credentials" {
		t.Errorf("error = %q, want %q", result["error"], "invalid credentials")
	}
}

func TestLoginEmptyKeyReturns400(t *testing.T) {
	b := newTestBrokerWithAuth(t, "empty-test-key", "admin", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	body, _ := json.Marshal(map[string]string{"api_key": ""})
	resp, err := http.Post("http://"+httpAddr+"/dashboard/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /dashboard/login: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("login status: %d, want 400", resp.StatusCode)
	}
}

func TestLoginInvalidJSONReturns400(t *testing.T) {
	b := newTestBrokerWithAuth(t, "json-test-key", "admin", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	resp, err := http.Post("http://"+httpAddr+"/dashboard/login", "application/json", strings.NewReader("not-json"))
	if err != nil {
		t.Fatalf("POST /dashboard/login: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("login status: %d, want 400", resp.StatusCode)
	}
}

func TestLogoutReturnsClearCookie(t *testing.T) {
	b := newTestBrokerWithAuth(t, "logout-cookie-key", "looper2", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	cookieVal := loginAndCookie(t, httpAddr, "logout-cookie-key")

	req, _ := http.NewRequest("POST", "http://"+httpAddr+"/dashboard/logout", nil)
	req.Header.Set("Cookie", "pubsub_dashboard_session="+cookieVal)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /dashboard/logout: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("logout status: %d, want 200", resp.StatusCode)
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result["status"] != "ok" {
		t.Errorf("logout status = %q, want %q", result["status"], "ok")
	}

	found := false
	for _, c := range resp.Cookies() {
		if c.Name == "pubsub_dashboard_session" {
			found = true
			if c.MaxAge != -1 {
				t.Errorf("clear cookie MaxAge = %d, want -1", c.MaxAge)
			}
		}
	}
	if !found {
		t.Error("response missing clear cookie for pubsub_dashboard_session")
	}
}

func TestDashboardAfterLoginShowsIndexHTML(t *testing.T) {
	b := newTestBrokerWithAuth(t, "dash-login-key", "dash-user2", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	cookieVal := loginAndCookie(t, httpAddr, "dash-login-key")

	client := &http.Client{
		Jar: newCookieJar("pubsub_dashboard_session", cookieVal),
	}
	resp, err := client.Get("http://" + httpAddr + "/dashboard")
	if err != nil {
		t.Fatalf("GET /dashboard: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /dashboard: status %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "Control Center") {
		t.Error("GET /dashboard after login: missing 'Control Center' (should serve index.html, not login)")
	}
}

func TestDashboardAfterLogoutShowsLoginPage(t *testing.T) {
	b := newTestBrokerWithAuth(t, "dash-logout-key", "dash-user3", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	cookieVal := loginAndCookie(t, httpAddr, "dash-logout-key")

	// Logout
	req, _ := http.NewRequest("POST", "http://"+httpAddr+"/dashboard/logout", nil)
	req.Header.Set("Cookie", "pubsub_dashboard_session="+cookieVal)
	logoutResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /dashboard/logout: %v", err)
	}
	logoutResp.Body.Close()

	// After logout, GET /dashboard should show login page
	resp, err := http.Get("http://" + httpAddr + "/dashboard")
	if err != nil {
		t.Fatalf("GET /dashboard after logout: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /dashboard after logout: status %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	if !strings.Contains(bodyStr, "loginForm") {
		t.Error("GET /dashboard after logout: should show login form")
	}
	if strings.Contains(bodyStr, "Control Center") {
		// The login page also contains "Control Center" now, so check for login-specific elements
	}
	if !strings.Contains(bodyStr, "Sign In") {
		t.Error("GET /dashboard after logout: should show 'Sign In' button")
	}
}

func TestExpiredSessionShowsLoginPage(t *testing.T) {
	dir := t.TempDir()
	cfgData := fmt.Sprintf(`{
		"broker":  {"node_id": "exp-show-login"},
		"network": {"port": 0, "max_connections": 100,
		            "dashboard_session_ttl": 100000000,
		            "read_timeout": 5000000000, "write_timeout": 5000000000},
		"storage": {"wal_path": %q, "data_path": %q,
		            "segment_max_bytes": 1048576, "index_interval_bytes": 512, "sync_policy": "always"},
		"auth": {
			"enabled": true,
			"api_keys": [{"key": "exp-login-key", "client_id": "exp-user", "role": "admin"}]
		},
		"rate_limit": {"enabled": false},
		"logging":    {"level": "error", "format": "json"}
	}`, filepath.Join(dir, "wal"), filepath.Join(dir, "data"))

	cfgPath := filepath.Join(dir, "config.json")
	os.WriteFile(cfgPath, []byte(cfgData), 0o644)
	loader, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	t.Cleanup(loader.Close)
	b, err := broker.New(loader)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	// Wait for session to expire (100ms TTL).
	time.Sleep(150 * time.Millisecond)

	// After expiry, GET /dashboard should show login page.
	resp, err := http.Get("http://" + httpAddr + "/dashboard")
	if err != nil {
		t.Fatalf("GET /dashboard: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /dashboard expired: status %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "loginForm") {
		t.Error("GET /dashboard with expired session: should show login form")
	}
}

func TestLoginEmptyBodyReturns400(t *testing.T) {
	b := newTestBrokerWithAuth(t, "empty-body-key", "admin", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	resp, err := http.Post("http://"+httpAddr+"/dashboard/login", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST /dashboard/login: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("login status: %d, want 400", resp.StatusCode)
	}
}

func TestLoginHTMLContainsDesignElements(t *testing.T) {
	html := broker.LoginHTML()
	if len(html) == 0 {
		t.Fatal("LoginHTML() returned empty")
	}
	body := string(html)

	// Verify dashboard design tokens are present
	designChecks := []struct {
		name, pattern string
	}{
		{"dashboard bg color", "--bg:#09090b"},
		{"dashboard bg2 color", "--bg2:#18181b"},
		{"dashboard border color", "--border:#3f3f46"},
		{"dashboard accent color", "--accent:#22c55e"},
		{"login form", "loginForm"},
		{"password toggle", "toggleKey"},
		{"remember me", "rememberMe"},
		{"submit button", "submitBtn"},
		{"error banner", "errorBanner"},
		{"loading spinner", "spinner"},
		{"aria-required", "aria-required"},
		{"aria-label", "aria-label"},
		{"aria-live", "aria-live"},
		{"responsive meta", "viewport"},
	}
	for _, check := range designChecks {
		if !strings.Contains(body, check.pattern) {
			t.Errorf("login.html missing %s (%q)", check.name, check.pattern)
		}
	}
}

// cookieJar is a minimal http.CookieJar for tests.
type cookieJar struct {
	name, value string
}

func newCookieJar(name, value string) *cookieJar {
	return &cookieJar{name: name, value: value}
}

func (j *cookieJar) SetCookies(u *url.URL, cookies []*http.Cookie) {}

func (j *cookieJar) Cookies(u *url.URL) []*http.Cookie {
	return []*http.Cookie{{Name: j.name, Value: j.value}}
}

func TestRefreshAfterLogoutShowsLoginPage(t *testing.T) {
	b := newTestBrokerWithAuth(t, "refresh-logout-key", "refresh-user", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	cookieVal := loginAndCookie(t, httpAddr, "refresh-logout-key")

	// Logout.
	req, _ := http.NewRequest("POST", "http://"+httpAddr+"/dashboard/logout", nil)
	req.Header.Set("Cookie", "pubsub_dashboard_session="+cookieVal)
	logoutResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /dashboard/logout: %v", err)
	}
	logoutResp.Body.Close()

	// Simulate browser refresh (GET /dashboard with no cookie).
	resp, err := http.Get("http://" + httpAddr + "/dashboard")
	if err != nil {
		t.Fatalf("GET /dashboard after logout: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /dashboard after logout: status %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)
	if !strings.Contains(bodyStr, "loginForm") {
		t.Error("GET /dashboard after logout: should show login form")
	}
	if !strings.Contains(bodyStr, "Sign In") {
		t.Error("GET /dashboard after logout: should show 'Sign In' button")
	}
}

func TestRefreshAfterLoginShowsDashboard(t *testing.T) {
	b := newTestBrokerWithAuth(t, "refresh-login-key", "refresh-user2", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	cookieVal := loginAndCookie(t, httpAddr, "refresh-login-key")

	// Simulate browser refresh (GET /dashboard WITH session cookie).
	client := &http.Client{
		Jar: newCookieJar("pubsub_dashboard_session", cookieVal),
	}
	resp, err := client.Get("http://" + httpAddr + "/dashboard")
	if err != nil {
		t.Fatalf("GET /dashboard after login: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /dashboard after login: status %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "Control Center") {
		t.Error("GET /dashboard after login: should show dashboard")
	}
	if strings.Contains(string(body), "loginForm") {
		t.Error("GET /dashboard after login: should NOT show login form")
	}
}

func TestUnauthorizedAccessAfterLogout(t *testing.T) {
	b := newTestBrokerWithAuth(t, "unauth-logout-key", "unauth-user", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	cookieVal := loginAndCookie(t, httpAddr, "unauth-logout-key")

	// Verify session works.
	client := &http.Client{
		Jar: newCookieJar("pubsub_dashboard_session", cookieVal),
	}
	resp, err := client.Get("http://" + httpAddr + "/topics")
	if err != nil {
		t.Fatalf("GET /topics: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /topics before logout: status %d, want 200", resp.StatusCode)
	}

	// Logout.
	req, _ := http.NewRequest("POST", "http://"+httpAddr+"/dashboard/logout", nil)
	req.Header.Set("Cookie", "pubsub_dashboard_session="+cookieVal)
	logoutResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /dashboard/logout: %v", err)
	}
	logoutResp.Body.Close()

	// Attempt to access protected API after logout — should fail.
	resp2, err := client.Get("http://" + httpAddr + "/topics")
	if err != nil {
		t.Fatalf("GET /topics after logout: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Errorf("GET /topics after logout: status %d, want 401", resp2.StatusCode)
	}

	// Attempt to access /dashboard/session — should return 401.
	resp3, err := client.Get("http://" + httpAddr + "/dashboard/session")
	if err != nil {
		t.Fatalf("GET /dashboard/session after logout: %v", err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusUnauthorized {
		t.Errorf("GET /dashboard/session after logout: status %d, want 401", resp3.StatusCode)
	}
}

func TestDashboardHTMLCacheControlHeaders(t *testing.T) {
	b := newTestBrokerWithAuth(t, "cache-key", "cache-user", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	cookieVal := loginAndCookie(t, httpAddr, "cache-key")

	client := &http.Client{
		Jar: newCookieJar("pubsub_dashboard_session", cookieVal),
	}
	resp, err := client.Get("http://" + httpAddr + "/dashboard")
	if err != nil {
		t.Fatalf("GET /dashboard: %v", err)
	}
	defer resp.Body.Close()

	cc := resp.Header.Get("Cache-Control")
	if !strings.Contains(cc, "no-store") {
		t.Errorf("GET /dashboard Cache-Control = %q, want to contain 'no-store'", cc)
	}
	if !strings.Contains(cc, "no-cache") {
		t.Errorf("GET /dashboard Cache-Control = %q, want to contain 'no-cache'", cc)
	}
}

func TestLogoutEndpointAvailableWhenAuthDisabled(t *testing.T) {
	dir := t.TempDir()
	cfgData := fmt.Sprintf(`{
		"broker":  {"node_id": "noauth-logout-test"},
		"network": {"port": 0, "max_connections": 100,
		            "dashboard_enabled": true,
		            "read_timeout": 5000000000, "write_timeout": 5000000000},
		"storage": {"wal_path": %q, "data_path": %q,
		            "segment_max_bytes": 1048576, "index_interval_bytes": 512, "sync_policy": "always"},
		"auth":       {"enabled": false},
		"rate_limit": {"enabled": false},
		"logging":    {"level": "error", "format": "json"}
	}`, filepath.Join(dir, "wal"), filepath.Join(dir, "data"))

	cfgPath := filepath.Join(dir, "config.json")
	os.WriteFile(cfgPath, []byte(cfgData), 0o644)
	loader, _ := config.Load(cfgPath)
	t.Cleanup(loader.Close)
	b, _ := broker.New(loader)
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	// POST /dashboard/logout should return 200 (not 404) even when auth is off.
	resp, err := http.Post("http://"+httpAddr+"/dashboard/logout", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("POST /dashboard/logout: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /dashboard/logout with auth disabled: status %d, want 200", resp.StatusCode)
	}
}

func TestLogoutClearsSameSiteAttribute(t *testing.T) {
	b := newTestBrokerWithAuth(t, "samesite-key", "ss-user", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	cookieVal := loginAndCookie(t, httpAddr, "samesite-key")

	req, _ := http.NewRequest("POST", "http://"+httpAddr+"/dashboard/logout", nil)
	req.Header.Set("Cookie", "pubsub_dashboard_session="+cookieVal)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /dashboard/logout: %v", err)
	}
	defer resp.Body.Close()

	found := false
	for _, c := range resp.Cookies() {
		if c.Name == "pubsub_dashboard_session" {
			found = true
			if c.SameSite != http.SameSiteStrictMode {
				t.Errorf("clear cookie SameSite = %v, want SameSiteStrictMode", c.SameSite)
			}
			if c.MaxAge != -1 {
				t.Errorf("clear cookie MaxAge = %d, want -1", c.MaxAge)
			}
		}
	}
	if !found {
		t.Error("response missing clear cookie for pubsub_dashboard_session")
	}
}

func TestLoginRedirectAfterLogout(t *testing.T) {
	b := newTestBrokerWithAuth(t, "redir-key", "redir-user", "admin")
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })
	httpAddr := waitForHTTP(t, b)

	cookieVal := loginAndCookie(t, httpAddr, "redir-key")

	// Logout.
	req, _ := http.NewRequest("POST", "http://"+httpAddr+"/dashboard/logout", nil)
	req.Header.Set("Cookie", "pubsub_dashboard_session="+cookieVal)
	logoutResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /dashboard/logout: %v", err)
	}
	logoutResp.Body.Close()

	// Login again with the same key.
	cookieVal2 := loginAndCookie(t, httpAddr, "redir-key")
	if cookieVal2 == "" {
		t.Fatal("re-login failed: empty cookie")
	}

	// Verify re-login works — session is valid.
	client := &http.Client{
		Jar: newCookieJar("pubsub_dashboard_session", cookieVal2),
	}
	resp, err := client.Get("http://" + httpAddr + "/dashboard/session")
	if err != nil {
		t.Fatalf("GET /dashboard/session after re-login: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /dashboard/session after re-login: status %d, want 200", resp.StatusCode)
	}
	var sessInfo struct {
		ClientID string `json:"client_id"`
		Role     string `json:"role"`
	}
	json.NewDecoder(resp.Body).Decode(&sessInfo)
	if sessInfo.ClientID != "redir-user" {
		t.Errorf("session client_id = %q, want %q", sessInfo.ClientID, "redir-user")
	}
}
