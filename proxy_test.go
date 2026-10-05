package containerproxy

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"github.com/docker/docker/api/types/container"
)

func TestUpstreamDialAddressUsesSocketAddress(t *testing.T) {
	for _, test := range []struct {
		origin string
		want   string
	}{
		{origin: "http://127.0.0.1", want: "127.0.0.1:80"},
		{origin: "http://backend.internal:8080", want: "backend.internal:8080"},
		{origin: "https://backend.internal", want: "backend.internal:443"},
		{origin: "https://[2001:db8::1]:8443", want: "[2001:db8::1]:8443"},
		{origin: "http://[::1]", want: "[::1]:80"},
	} {
		t.Run(test.origin, func(t *testing.T) {
			parsed, err := url.Parse(test.origin)
			if err != nil {
				t.Fatal(err)
			}
			if got := upstreamDialAddress(parsed); got != test.want {
				t.Fatalf("dial address = %q; want %q", got, test.want)
			}
		})
	}
}

func TestRealReverseProxyHTTPAndHTTPSRequests(t *testing.T) {
	for _, scheme := range []string{"http", "https"} {
		t.Run(scheme, func(t *testing.T) {
			requestTarget := make(chan string, 1)
			upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestTarget <- r.URL.RequestURI()
				_, _ = io.WriteString(w, "proxied")
			}))
			if scheme == "https" {
				upstream.StartTLS()
			} else {
				upstream.Start()
			}
			defer upstream.Close()

			fake := newFakeDocker(container.StateRunning)
			setDockerFactory(t, func() (dockerAPI, error) { return fake, nil })
			config := proxyTestConfig(t, upstream.URL, freeTCPAddress(t))
			if err := caddy.Load(config, true); err != nil {
				t.Fatalf("loading active Caddy config: %v", err)
			}
			t.Cleanup(func() {
				if err := caddy.Stop(); err != nil {
					t.Errorf("stopping Caddy: %v", err)
				}
			})

			proxyHandler := activeProxyHandler(t)
			if scheme == "https" {
				transport, ok := proxyHandler.proxy.Transport.(*retryTransport)
				if !ok {
					t.Fatalf("proxy transport = %T; want retry transport", proxyHandler.proxy.Transport)
				}
				caddyTransport, ok := transport.next.(*reverseproxy.HTTPTransport)
				if !ok || caddyTransport.TLS == nil || caddyTransport.Transport.TLSClientConfig == nil {
					t.Fatal("HTTPS upstream did not provision Caddy's TLS-enabled HTTP transport")
				}
				roots := x509.NewCertPool()
				roots.AddCert(upstream.Certificate())
				caddyTransport.Transport.TLSClientConfig.RootCAs = roots
			}

			serverAddress := activeHTTPServer(t).Listeners()[0].Addr().String()
			response, err := http.Get("http://" + serverAddress + "/asset?id=42")
			if err != nil {
				t.Fatal(err)
			}
			body, readErr := io.ReadAll(response.Body)
			closeErr := response.Body.Close()
			if readErr != nil || closeErr != nil {
				t.Fatalf("reading proxy response: read=%v close=%v", readErr, closeErr)
			}
			if response.StatusCode != http.StatusOK || string(body) != "proxied" {
				t.Fatalf("proxy response = %d %q; want 200 %q", response.StatusCode, body, "proxied")
			}
			pathAndQuery := <-requestTarget
			if pathAndQuery != "/asset?id=42" {
				t.Fatalf("upstream received %q; want original path and query", pathAndQuery)
			}
			if err := caddy.Stop(); err != nil {
				t.Fatalf("stopping active Caddy config: %v", err)
			}
			fake.mu.Lock()
			stops := fake.stops
			fake.mu.Unlock()
			if stops != 1 {
				t.Fatalf("active config cleanup issued %d Docker stops; want 1", stops)
			}
		})
	}
}

func TestCaddyValidationCleanupDoesNotStopContainers(t *testing.T) {
	fake := newFakeDocker(container.StateRunning)
	setDockerFactory(t, func() (dockerAPI, error) { return fake, nil })
	configJSON := proxyTestConfig(t, "http://127.0.0.1:8080", freeTCPAddress(t))
	var config caddy.Config
	if err := json.Unmarshal(configJSON, &config); err != nil {
		t.Fatal(err)
	}
	if err := caddy.Validate(&config); err != nil {
		t.Fatalf("validating Caddy config: %v", err)
	}
	fake.mu.Lock()
	starts, stops, closes := fake.starts, fake.stops, fake.closes
	fake.mu.Unlock()
	if starts != 0 || stops != 0 || closes != 1 {
		t.Fatalf("validation Docker starts/stops/closes = %d/%d/%d; want 0/0/1", starts, stops, closes)
	}
}

func TestReloadKeepsSharedLifecycleUntilFinalStop(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()

	firstDocker := newFakeDocker(container.StateRunning)
	secondDocker := newFakeDocker(container.StateRunning)
	var factoryCalls atomic.Int32
	setDockerFactory(t, func() (dockerAPI, error) {
		if factoryCalls.Add(1) == 1 {
			return firstDocker, nil
		}
		return secondDocker, nil
	})
	config := proxyTestConfig(t, upstream.URL, freeTCPAddress(t))
	if err := caddy.Load(config, true); err != nil {
		t.Fatalf("loading initial config: %v", err)
	}
	t.Cleanup(func() {
		if err := caddy.Stop(); err != nil {
			t.Errorf("stopping Caddy: %v", err)
		}
	})
	if err := caddy.Load(config, true); err != nil {
		t.Fatalf("reloading config: %v", err)
	}
	firstDocker.mu.Lock()
	firstStops := firstDocker.stops
	firstDocker.mu.Unlock()
	if firstStops != 0 {
		t.Fatalf("reload stopped shared container %d times; want 0", firstStops)
	}
	secondDocker.mu.Lock()
	secondCloses := secondDocker.closes
	secondDocker.mu.Unlock()
	if secondCloses != 1 {
		t.Fatalf("unused reload Docker client closed %d times; want 1", secondCloses)
	}
	if err := caddy.Stop(); err != nil {
		t.Fatalf("stopping final config: %v", err)
	}
	firstDocker.mu.Lock()
	firstStops = firstDocker.stops
	firstDocker.mu.Unlock()
	if firstStops != 1 {
		t.Fatalf("final shutdown stopped shared container %d times; want 1", firstStops)
	}
}

func proxyTestConfig(t *testing.T, upstream, listen string) []byte {
	t.Helper()
	config := map[string]any{"apps": map[string]any{
		"http": map[string]any{"servers": map[string]any{"test": map[string]any{
			"listen": []string{listen},
			"routes": []any{map[string]any{"handle": []any{map[string]any{
				"handler": "container_proxy", "upstream": upstream, "container": "test-id", "idle_timeout": "1h",
			}}}},
		}}},
	}}
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func freeTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func activeHTTPServer(t *testing.T) *caddyhttp.Server {
	t.Helper()
	module, err := caddy.ActiveContext().App("http")
	if err != nil {
		t.Fatal(err)
	}
	httpApp := module.(*caddyhttp.App)
	for _, server := range httpApp.Servers {
		return server
	}
	t.Fatal("active Caddy HTTP app has no servers")
	return nil
}

func activeProxyHandler(t *testing.T) *Handler {
	t.Helper()
	server := activeHTTPServer(t)
	for _, route := range server.Routes {
		for _, handler := range route.Handlers {
			if proxyHandler, ok := handler.(*Handler); ok {
				return proxyHandler
			}
		}
	}
	t.Fatal("active Caddy server has no container_proxy handler")
	return nil
}

func setDockerFactory(t *testing.T, factory func() (dockerAPI, error)) {
	t.Helper()
	previous := newDockerClient
	newDockerClient = factory
	t.Cleanup(func() { newDockerClient = previous })
}

func TestColdSafeRequestRetriesStartupResponses(t *testing.T) {
	var attempts atomic.Int32
	var discardedClosed atomic.Bool
	transport := &retryTransport{
		retries: 1,
		backoff: time.Millisecond,
		next: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			if attempts.Add(1) == 1 {
				return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: closeNotifyBody{closed: &discardedClosed}, Header: make(http.Header), Request: request}, nil
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ready")), Header: make(http.Header), Request: request}, nil
		}),
	}

	response := roundTripForTest(t, transport, http.MethodGet, true, true)
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d; want 200", response.StatusCode)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d; want 2", got)
	}
	if !discardedClosed.Load() {
		t.Fatal("discarded response body was not closed")
	}
}

func TestWarmAndPotentiallySideEffectingRequestsAreNotRetried(t *testing.T) {
	for _, test := range []struct {
		name   string
		method string
		cold   bool
	}{
		{name: "warm GET", method: http.MethodGet, cold: false},
		{name: "cold POST", method: http.MethodPost, cold: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var attempts atomic.Int32
			transport := &retryTransport{
				retries: 3,
				backoff: time.Millisecond,
				next: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
					attempts.Add(1)
					return &http.Response{StatusCode: http.StatusBadGateway, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header), Request: request}, nil
				}),
			}
			response := roundTripForTest(t, transport, test.method, test.cold, true)
			response.Body.Close()
			if got := attempts.Load(); got != 1 {
				t.Fatalf("attempts = %d; want 1", got)
			}
		})
	}
}

func TestHeaderDeadlineStopsWhenResponseHeadersArrive(t *testing.T) {
	budget := newHeaderBudget(context.Background(), 20*time.Millisecond)
	defer budget.finish()
	request := requestWithBudget(budget, http.MethodGet, true, true)
	transport := &retryTransport{next: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: &requestContextBody{ctx: request.Context()}, Header: make(http.Header), Request: request}, nil
	})}
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	time.Sleep(35 * time.Millisecond)
	if budget.expired.Load() || budget.ctx.Err() != nil {
		t.Fatal("header deadline canceled a response after headers had arrived")
	}
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "stream" {
		t.Fatalf("stream body = %q, %v; want it to remain readable after the header deadline", body, err)
	}
}

func TestHeaderDeadlineReturns500BeforeHeaders(t *testing.T) {
	budget := newHeaderBudget(context.Background(), 20*time.Millisecond)
	defer budget.finish()
	request := requestWithBudget(budget, http.MethodGet, true, true)
	transport := &retryTransport{next: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d; want 500", response.StatusCode)
	}
}

func roundTripForTest(t *testing.T, transport *retryTransport, method string, cold, safe bool) *http.Response {
	t.Helper()
	budget := newHeaderBudget(context.Background(), time.Second)
	t.Cleanup(budget.finish)
	response, err := transport.RoundTrip(requestWithBudget(budget, method, cold, safe))
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func requestWithBudget(budget *headerBudget, method string, cold, safe bool) *http.Request {
	ctx := context.WithValue(budget.ctx, coldRequestKey{}, cold)
	ctx = context.WithValue(ctx, retrySafeRequestKey{}, safe)
	return (&http.Request{Method: method, URL: mustURL("http://upstream.invalid"), Header: make(http.Header), Body: http.NoBody}).WithContext(ctx)
}

func mustURL(value string) *url.URL {
	parsed, err := url.Parse(value)
	if err != nil {
		panic(err)
	}
	return parsed
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type closeNotifyBody struct {
	closed *atomic.Bool
}

type requestContextBody struct {
	ctx  context.Context
	done bool
}

func (b *requestContextBody) Read(destination []byte) (int, error) {
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	if b.done {
		return 0, io.EOF
	}
	b.done = true
	return copy(destination, "stream"), nil
}

func (*requestContextBody) Close() error { return nil }

func (b closeNotifyBody) Read([]byte) (int, error) { return 0, errors.New("body should not be read") }
func (b closeNotifyBody) Close() error {
	b.closed.Store(true)
	return nil
}
