package containerproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp/reverseproxy"
	"go.uber.org/zap"
)

var lifecycleManagers = caddy.NewUsagePool()

func init() {
	caddy.RegisterModule(Handler{})
}

// Handler starts an existing Docker container on demand and proxies to it.
type Handler struct {
	Upstream       string          `json:"upstream,omitempty"`
	Container      string          `json:"container,omitempty"`
	IdleTimeout    caddy.Duration  `json:"idle_timeout,omitempty"`
	Timeout        *caddy.Duration `json:"timeout,omitempty"`
	HealthEndpoint string          `json:"health_endpoint,omitempty"`
	StartupDelay   caddy.Duration  `json:"startup_delay,omitempty"`
	Retries        int             `json:"retries,omitempty"`
	RetryBackoff   *caddy.Duration `json:"retry_backoff,omitempty"`

	proxy      *reverseproxy.Handler
	manager    *manager
	managerKey managerRegistryKey
	registered bool
	logger     *zap.Logger
}

func (Handler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.container_proxy",
		New: func() caddy.Module { return new(Handler) },
	}
}

func (h *Handler) Provision(ctx caddy.Context) error {
	if err := h.Validate(); err != nil {
		return err
	}
	policy, err := policyFor(h)
	if err != nil {
		return err
	}
	h.logger = ctx.Logger()
	lifecycleAppModule, err := ctx.App(lifecycleAppID)
	if err != nil {
		return fmt.Errorf("loading container lifecycle app: %w", err)
	}
	lifecycleApp := lifecycleAppModule.(*lifecycleApp)

	docker, err := newDockerClient()
	if err != nil {
		return fmt.Errorf("creating Docker client: %w", err)
	}
	clientOwned := true
	defer func() {
		if clientOwned {
			_ = docker.Close()
		}
	}()

	dockerCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := docker.Ping(dockerCtx); err != nil {
		return fmt.Errorf("connecting to Docker Engine: %w", err)
	}
	info, err := inspectContainer(dockerCtx, docker, h.Container)
	if err != nil {
		return fmt.Errorf("container %q is unavailable: %w", h.Container, err)
	}
	state, err := initialManagerState(info.State)
	if err != nil {
		return fmt.Errorf("container %q cannot be managed: %w", h.Container, err)
	}

	transport := &reverseproxy.HTTPTransport{}
	upstream, _ := url.Parse(h.Upstream)
	if upstream.Scheme == "https" {
		transport.TLS = &reverseproxy.TLSConfig{}
	}
	proxy := &reverseproxy.Handler{
		Upstreams:    reverseproxy.UpstreamPool{{Dial: upstreamDialAddress(upstream)}},
		TransportRaw: caddyconfig.JSONModuleObject(transport, "protocol", "http", nil),
	}
	if err := proxy.Provision(ctx); err != nil {
		_ = proxy.Cleanup()
		return fmt.Errorf("provisioning reverse proxy: %w", err)
	}
	proxy.Transport = &retryTransport{
		next:    proxy.Transport,
		retries: h.Retries,
		backoff: time.Duration(*h.RetryBackoff),
		logger:  h.logger,
	}

	reference, identity := containerReference(h.Container, info)
	key := dockerEndpointKey(docker, identity)
	pooled, loaded, err := lifecycleManagers.LoadOrNew(key, func() (caddy.Destructor, error) {
		manager := newManager(docker, info.ID, policy, state, h.logger.Named("lifecycle"))
		manager.containerRef = reference
		return manager, nil
	})
	if err != nil {
		_ = proxy.Cleanup()
		return fmt.Errorf("registering container lifecycle manager: %w", err)
	}
	if loaded {
		_ = docker.Close()
	}
	clientOwned = false
	h.manager = pooled.(*manager)
	if err := h.manager.checkRegistration(policy, reference); err != nil {
		_, releaseErr := lifecycleManagers.Delete(key)
		h.manager = nil
		_ = proxy.Cleanup()
		return errors.Join(err, releaseErr)
	}
	if err := lifecycleApp.register(h.manager); err != nil {
		_, releaseErr := lifecycleManagers.Delete(key)
		h.manager = nil
		_ = proxy.Cleanup()
		return errors.Join(err, releaseErr)
	}
	h.proxy = proxy
	h.managerKey = key
	h.registered = true
	return nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	budget := newHeaderBudget(r.Context(), time.Duration(*h.Timeout))
	defer budget.finish()
	cold, err := h.manager.Acquire(budget.ctx)
	if err != nil {
		if r.Context().Err() != nil {
			return nil
		}
		if budget.expired.Load() {
			err = context.DeadlineExceeded
		}
		h.logger.Error("container unavailable for request", zap.String("container", h.manager.containerRef), zap.String("container_id", h.manager.currentID()), zap.Error(err))
		return caddyhttp.Error(http.StatusInternalServerError, err)
	}
	defer h.manager.Release()
	requestCtx := context.WithValue(budget.ctx, coldRequestKey{}, cold)
	requestCtx = context.WithValue(requestCtx, retrySafeRequestKey{}, requestHasNoBody(r))
	requestCtx = context.WithValue(requestCtx, managerRequestKey{}, h.manager)
	request := r.WithContext(requestCtx)
	return h.proxy.ServeHTTP(w, request, next)
}

func (h *Handler) Cleanup() error {
	var cleanupErr error
	if h.registered {
		_, cleanupErr = lifecycleManagers.Delete(h.managerKey)
		h.registered = false
	}
	if h.proxy != nil {
		cleanupErr = errors.Join(cleanupErr, h.proxy.Cleanup())
	}
	return cleanupErr
}

func upstreamDialAddress(upstream *url.URL) string {
	port := upstream.Port()
	if port == "" {
		if upstream.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return net.JoinHostPort(upstream.Hostname(), port)
}

type coldRequestKey struct{}

type retrySafeRequestKey struct{}

type managerRequestKey struct{}

type headerBudgetKey struct{}

type headerBudget struct {
	ctx     context.Context
	cancel  context.CancelCauseFunc
	timer   *time.Timer
	expired atomic.Bool
	timerMu sync.Mutex
	stopped bool
}

func newHeaderBudget(parent context.Context, duration time.Duration) *headerBudget {
	ctx, cancel := context.WithCancelCause(parent)
	budget := &headerBudget{cancel: cancel}
	budget.ctx = context.WithValue(ctx, headerBudgetKey{}, budget)
	budget.timer = time.AfterFunc(duration, func() {
		budget.timerMu.Lock()
		defer budget.timerMu.Unlock()
		if budget.stopped {
			return
		}
		budget.expired.Store(true)
		cancel(context.DeadlineExceeded)
	})
	return budget
}

func (b *headerBudget) stopTimer() {
	b.timerMu.Lock()
	b.stopped = true
	b.timer.Stop()
	b.timerMu.Unlock()
}

func (b *headerBudget) finish() {
	b.stopTimer()
	b.cancel(context.Canceled)
}

type retryTransport struct {
	next    http.RoundTripper
	retries int
	backoff time.Duration
	logger  *zap.Logger
}

func (t *retryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	budget, _ := request.Context().Value(headerBudgetKey{}).(*headerBudget)
	cold, _ := request.Context().Value(coldRequestKey{}).(bool)
	safeBody, _ := request.Context().Value(retrySafeRequestKey{}).(bool)
	retryable := cold && safeBody && (request.Method == http.MethodGet || request.Method == http.MethodHead)
	maxRetries := 0
	if retryable {
		maxRetries = t.retries
	}

	for attempt := 0; ; attempt++ {
		response, err := t.next.RoundTrip(request)
		if err != nil && response != nil && response.Body != nil {
			_ = response.Body.Close()
			response = nil
		}
		if err == nil && response == nil {
			err = errors.New("upstream transport returned neither a response nor an error")
		}
		if budget != nil && budget.expired.Load() {
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			return deadlineResponse(request), nil
		}

		shouldRetry := attempt < maxRetries && (err != nil || retryableStatus(response))
		if !shouldRetry {
			if err != nil {
				if lifecycle, ok := request.Context().Value(managerRequestKey{}).(*manager); ok {
					lifecycle.markUnready()
				}
			}
			if err == nil && budget != nil {
				budget.stopTimer()
			}
			return response, err
		}

		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		if t.logger != nil {
			t.logger.Debug("retrying cold container request", zap.Int("attempt", attempt+1))
		}
		if err := sleepContext(request.Context(), t.backoff); err != nil {
			if budget != nil && budget.expired.Load() {
				return deadlineResponse(request), nil
			}
			return nil, err
		}
	}
}

func requestHasNoBody(request *http.Request) bool {
	return (request.Body == nil || request.Body == http.NoBody) && request.ContentLength == 0 && len(request.TransferEncoding) == 0
}

func retryableStatus(response *http.Response) bool {
	if response == nil {
		return false
	}
	switch response.StatusCode {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func deadlineResponse(request *http.Request) *http.Response {
	body := io.NopCloser(strings.NewReader("request deadline exceeded\n"))
	return &http.Response{
		Status:        "500 Internal Server Error",
		StatusCode:    http.StatusInternalServerError,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        make(http.Header),
		Body:          body,
		ContentLength: int64(len("request deadline exceeded\n")),
		Request:       request,
	}
}

var _ caddy.Module = (*Handler)(nil)
var _ caddy.Provisioner = (*Handler)(nil)
var _ caddy.Validator = (*Handler)(nil)
var _ caddy.CleanerUpper = (*Handler)(nil)
var _ caddyhttp.MiddlewareHandler = (*Handler)(nil)
