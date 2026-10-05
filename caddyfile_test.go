package containerproxy

import (
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
)

func TestContainerProxyCaddyfileOptions(t *testing.T) {
	input := `container_proxy https://photos.internal:8443 {
	container photos
	idle_timeout 15m
	timeout 10s
	health_endpoint /health/ready
	startup_delay 2s
	retries 2
	retry_backoff 250ms
}`
	tokens, err := caddyfile.Tokenize([]byte(input), "Caddyfile")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseCaddyfile(httpcaddyfile.Helper{Dispenser: caddyfile.NewDispenser(tokens)})
	if err != nil {
		t.Fatal(err)
	}
	handler := parsed.(*Handler)
	if handler.Upstream != "https://photos.internal:8443" || handler.Container != "photos" ||
		time.Duration(handler.IdleTimeout) != 15*time.Minute || time.Duration(*handler.Timeout) != 10*time.Second ||
		handler.HealthEndpoint != "/health/ready" || time.Duration(handler.StartupDelay) != 2*time.Second ||
		handler.Retries != 2 || time.Duration(*handler.RetryBackoff) != 250*time.Millisecond {
		t.Fatalf("unexpected parsed handler configuration: %+v", handler)
	}
}

func TestContainerProxyAdaptsAsHTTPHandler(t *testing.T) {
	adapter := caddyfile.Adapter{ServerType: httpcaddyfile.ServerType{}}
	input := `photos.example.com {
    container_proxy http://127.0.0.1:8080 {
        container photos
        idle_timeout 15m
    }
}`
	config, _, err := adapter.Adapt([]byte(input), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(config), `"handler":"container_proxy"`) {
		t.Fatalf("adapted Caddy JSON does not contain the handler: %s", config)
	}
}

func TestContainerProxyRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Handler)
	}{
		{name: "missing container", edit: func(h *Handler) { h.Container = "" }},
		{name: "nonpositive idle timeout", edit: func(h *Handler) { h.IdleTimeout = 0 }},
		{name: "unsupported upstream scheme", edit: func(h *Handler) { h.Upstream = "ftp://backend:21" }},
		{name: "upstream path", edit: func(h *Handler) { h.Upstream = "http://backend:80/base" }},
		{name: "invalid readiness path", edit: func(h *Handler) { h.HealthEndpoint = "https://backend/ready" }},
		{name: "negative retries", edit: func(h *Handler) { h.Retries = -1 }},
		{name: "zero timeout", edit: func(h *Handler) { zero := caddy.Duration(0); h.Timeout = &zero }},
		{name: "zero retry delay", edit: func(h *Handler) { zero := caddy.Duration(0); h.RetryBackoff = &zero; h.Retries = 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := &Handler{Upstream: "http://backend:8080", Container: "app", IdleTimeout: caddy.Duration(5 * time.Minute)}
			test.edit(handler)
			if err := handler.Validate(); err == nil {
				t.Fatal("invalid configuration passed validation")
			}
		})
	}
}
