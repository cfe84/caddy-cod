package containerproxy

import (
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

func init() {
	httpcaddyfile.RegisterHandlerDirective("container_proxy", parseCaddyfile)
	httpcaddyfile.RegisterDirectiveOrder("container_proxy", httpcaddyfile.After, "reverse_proxy")
}

func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	d := h.Dispenser
	if !d.Next() {
		return nil, d.Err("expected container_proxy directive")
	}

	module := &Handler{}
	if !d.NextArg() {
		return nil, d.ArgErr()
	}
	module.Upstream = d.Val()
	if d.NextArg() {
		return nil, d.Err("container_proxy accepts one upstream URL")
	}

	seen := make(map[string]bool)
	for d.NextBlock(0) {
		name := d.Val()
		if seen[name] {
			return nil, d.Errf("option %q can only be specified once", name)
		}
		seen[name] = true
		if !d.NextArg() {
			return nil, d.ArgErr()
		}
		value := d.Val()
		if d.NextArg() {
			return nil, d.Errf("option %q accepts one value", name)
		}

		switch name {
		case "container":
			module.Container = value
		case "idle_timeout":
			parsed, err := time.ParseDuration(value)
			if err != nil {
				return nil, d.Errf("invalid idle_timeout %q: %v", value, err)
			}
			module.IdleTimeout = caddy.Duration(parsed)
		case "timeout":
			parsed, err := time.ParseDuration(value)
			if err != nil {
				return nil, d.Errf("invalid timeout %q: %v", value, err)
			}
			if parsed <= 0 {
				return nil, d.Err("timeout must be positive")
			}
			duration := caddy.Duration(parsed)
			module.Timeout = &duration
		case "health_endpoint":
			module.HealthEndpoint = value
		case "startup_delay":
			parsed, err := time.ParseDuration(value)
			if err != nil {
				return nil, d.Errf("invalid startup_delay %q: %v", value, err)
			}
			module.StartupDelay = caddy.Duration(parsed)
		case "retries":
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return nil, d.Errf("invalid retries %q: %v", value, err)
			}
			module.Retries = parsed
		case "retry_backoff":
			parsed, err := time.ParseDuration(value)
			if err != nil {
				return nil, d.Errf("invalid retry_backoff %q: %v", value, err)
			}
			duration := caddy.Duration(parsed)
			module.RetryBackoff = &duration
		default:
			return nil, d.Errf("unknown container_proxy option %q", name)
		}
	}

	if err := module.Validate(); err != nil {
		return nil, d.WrapErr(err)
	}
	return module, nil
}

func (h *Handler) Validate() error {
	if h.Container == "" {
		return fmt.Errorf("container is required")
	}
	if h.IdleTimeout <= 0 {
		return fmt.Errorf("idle_timeout must be positive")
	}
	if h.Timeout == nil {
		defaultTimeout := caddy.Duration(10 * time.Second)
		h.Timeout = &defaultTimeout
	}
	if *h.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if h.RetryBackoff == nil {
		defaultBackoff := caddy.Duration(250 * time.Millisecond)
		h.RetryBackoff = &defaultBackoff
	}
	if h.StartupDelay < 0 {
		return fmt.Errorf("startup_delay cannot be negative")
	}
	if h.Retries < 0 {
		return fmt.Errorf("retries cannot be negative")
	}
	if *h.RetryBackoff < 0 || (h.Retries > 0 && *h.RetryBackoff == 0) {
		return fmt.Errorf("retry_backoff must be positive when retries are enabled")
	}

	upstream, err := url.Parse(h.Upstream)
	if err != nil || upstream.Host == "" || upstream.User != nil || upstream.RawQuery != "" || upstream.Fragment != "" || upstream.Path != "" {
		return fmt.Errorf("upstream must be an origin URL without credentials, path, query, or fragment")
	}
	if upstream.Scheme != "http" && upstream.Scheme != "https" {
		return fmt.Errorf("upstream scheme must be http or https")
	}
	if upstream.Hostname() == "" {
		return fmt.Errorf("upstream must include a host")
	}
	if port := upstream.Port(); port != "" {
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 1 || portNumber > 65535 {
			return fmt.Errorf("upstream port must be between 1 and 65535")
		}
		if (upstream.Scheme == "http" && portNumber == 443) || (upstream.Scheme == "https" && portNumber == 80) {
			return fmt.Errorf("upstream scheme conflicts with its port")
		}
	}

	if h.HealthEndpoint != "" {
		healthURL, err := url.ParseRequestURI(h.HealthEndpoint)
		if err != nil || healthURL == nil || healthURL.IsAbs() || healthURL.Host != "" || healthURL.Path == "" || healthURL.Path[0] != '/' || healthURL.RawQuery != "" || healthURL.Fragment != "" {
			return fmt.Errorf("health_endpoint must be an absolute path without a query or fragment")
		}
	}
	return nil
}
