# Demand-driven containers for Caddy

## Goal and scope

A Go extension for Caddy starts an existing container when a request arrives, proxies requests once it is ready, and stops it after a configurable period without traffic. Containers must already exist; the extension does not create, remove, or update them.

The initial implementation targets Docker Engine, including Docker-compatible engines where the required API behavior is supported. It uses the Docker Go client and Caddy's existing reverse proxy implementation rather than implementing either protocol itself.

`README.md` is the operator guide. This document records the architecture and
lifecycle decisions implemented by the module.

## Caddyfile configuration

```caddyfile
photos.example.com {
    container_proxy http://127.0.0.1:8080 {
        container photos
        idle_timeout 15m
        timeout 10s
        health_endpoint /health/ready
        startup_delay 2s
        retries 2
        retry_backoff 250ms
    }
}

reports.example.com {
    container_proxy http://127.0.0.1:8081 {
        container reports
        idle_timeout 30m
    }
}
```

The upstream is explicit: a container name alone does not identify its application port, network, or TLS requirements. It must be reachable from Caddy, for example through a published local port or a shared container network with a stable DNS name. Do not cache an inspected container IP across restarts.

| Option | Required / default | Meaning |
| --- | --- | --- |
| `container` | Required | Existing container name or ID. |
| `idle_timeout` | Required | Idle duration before stopping the container, such as `15m`. This replaces “cooldown-time.” |
| `timeout` | `10s` | Per-request deadline from arrival through startup, readiness, retries, and receipt of final upstream response headers. Expiry returns HTTP 500 if headers have not been sent. |
| `health_endpoint` | Optional | Absolute path on the configured upstream, such as `/health/ready`. HTTP 2xx means ready. Without it, Docker's `running` status is sufficient. |
| `startup_delay` | `0s` | Minimum wait after observing the container become running, before readiness checks or proxying. This replaces “boot-grace-period.” |
| `retries` | `0` | Maximum additional upstream attempts for eligible cold-start requests. Total attempts are `1 + retries`. |
| `retry_backoff` | `250ms` | Fixed delay between upstream retries. Must be positive when retries are enabled. |

Use Go duration syntax throughout; minutes for idle time and seconds for startup are conventions, not different parsing rules. Reject missing names/upstreams, multiple upstreams in the initial version, nonpositive idle/request timeouts, negative delays/retry counts, malformed health paths, and unsupported upstream schemes. Upstreams support HTTP and HTTPS; credentials and TLS behavior use Caddy's existing transport configuration where exposed.

Place the directive explicitly using `route` when ordering with other handlers matters. Register its ordinary directive order alongside `reverse_proxy`; inside a route, authentication and request rewriting can run before it. Health paths are relative to the upstream origin and do not inherit the incoming request path or query.

### Shared containers

Different endpoints may reference the same container. They share one lifecycle and aggregate active requests and idle time. Require their idle timeout, startup delay, upstream readiness target, and health endpoint to agree; reject conflicts during provisioning. Request timeout and retry policy may differ by route.

Resolve configured names to canonical Docker names for shared ownership across recreation and reloads. Expand ID references to full IDs and keep them pinned. Reject mixed name-based and ID-pinned registrations for the same container because they express incompatible replacement policies.

Inspect named containers on request admission, including warm requests, to detect replacement before forwarding. If the name is temporarily missing, poll within the request timeout. Adopt the replacement ID through the shared startup/readiness operation. Every Docker start, status wait, reconciliation, and stop is pinned to that operation's resolved ID; idle stop and cleanup never resolve a name to stop an unadopted replacement. A missing old ID during stop is already stopped. Initial provisioning still requires the configured container to exist.

## Runtime architecture

### Caddy modules

* **`http.handlers.container_proxy`** owns request admission, the request startup/header deadline, lifecycle leases, and configuration. It delegates HTTP proxying, streaming, forwarded headers, and upgrades to Caddy's `reverseproxy.Handler`.
* **A lifecycle manager** owns state and Docker operations for one container. A process-wide usage pool shares one manager per Docker endpoint and container ID across handlers and overlapping configurations.
* **The lifecycle app** records which managers belong to each Caddy configuration. Its `Start` and `Stop` methods activate idle management only for configurations that actually start; provisioning-only validation cleanup has no container side effects.
* **A transport wrapper** delegates to Caddy's configured HTTP transport and implements the narrowly scoped startup retries described below. It does not duplicate the reverse proxy's routing or response handling.

Integrate with Caddy's provisioning, validation, and cleanup interfaces. Acquire manager registrations during provisioning and release them during cleanup, including partial provisioning failure. Registry references must survive overlapping old/new configurations during a graceful reload, using Caddy's established shared-resource facilities. Conflicting lifecycle policies during that overlap fail reload rather than allowing two managers to control one container.

Keep three responsibilities separate: Docker operations, lifecycle decisions, and HTTP handling. The manager exposes reusable operations such as acquiring a request lease and querying state; idle cleanup and operational status use the same authoritative state.

### Docker access

Use the Docker client's established connection settings, normally the local Unix socket, with API version negotiation. Remote engines require the client's TLS/authentication configuration. Startup validates connectivity, inspects every configured container, and rejects missing containers or unsupported states. Do not start containers during configuration loading.

An active configuration opts its container into stop management even if it was already running when Caddy started. Provisioning and `caddy validate` do not start or stop containers. Docker access is privileged infrastructure access; do not expose lifecycle operations through a public HTTP endpoint.

## Lifecycle

The manager tracks `stopped`, `starting`, `ready`, `stopping`, and `unavailable`, plus an active lease count and last-idle timestamp. `ready` means the selected readiness policy passed, not merely that Docker reports the container running.

1. **Request admission:** Under the manager lock, acquire a lease before making any idle-stop decision. Cancel the idle timer. If ready, proceed; otherwise join or create one shared startup operation.
2. **Start:** Inspect current state. Start an exited/created container once; adopt a running container and validate readiness without restarting it. Paused, removing, or dead containers fail explicitly rather than being unpaused or recreated implicitly. Docker `restarting` state is observed until it becomes running or startup fails.
3. **Readiness:** After observing running, wait `startup_delay`. If configured, poll the health endpoint until it returns 2xx. Use a fixed internal polling interval, for example 250ms, short individual probe deadlines, and a bounded shared startup deadline. Probe through the underlying transport, bypassing demand admission and retries. Do not follow redirects; close all response bodies and avoid reading unbounded health content.
4. **Proxy:** Waiters proceed when ready. Hold the lease until the proxy handler returns, including streaming responses and upgraded connections.
5. **Release:** Always release the lease, including errors and client cancellation. When the final active request finishes, record the idle timestamp and arm the idle timer. This deliberately measures idle time after completion rather than request arrival, so long requests are not stopped mid-flight.
6. **Idle stop:** When the timer fires, recheck that no request is active and the idle duration has actually elapsed. Atomically enter `stopping`, then issue Docker's stop operation outside the lock. Use a finite grace period, initially 10s, and an API deadline longer than that grace period. Docker may kill the process after the grace period.
7. **Arrival during stop:** A request takes a lease and waits for stop to finish, then triggers/join a new startup. Never race start against a stop already dispatched; its request deadline still applies.

An initially running container with no traffic receives an idle timer when the configuration starts. Provisioning alone does not begin idle management. A stopped container stays stopped until demand arrives.

Only short state transitions occur under the lock. Docker calls, timers, health probes, and proxying run outside it. Startup/stop operations have generation tokens so late completion cannot overwrite a newer transition.

### Startup deadlines and cancellation

Each waiting request has its own configured deadline. Cancelling one waiter must not cancel startup for other waiters. A shared startup attempt has a finite deadline equal to the maximum request timeout registered for that manager, measured from the attempt's start. A later arrival does not extend it indefinitely.

If all request leases disappear, cancel outstanding readiness work. Inspect the result of any in-flight Docker operation before deciding the resulting state: cancellation does not guarantee Docker did not start the container. A container left running without demand enters idle-stop management.

A failed startup is shared with its current waiters; later demand can initiate a new attempt. Before retrying lifecycle operations, inspect actual Docker state rather than assuming a failed or timed-out call had no effect.

Stop failures retain managed state and schedule a bounded-rate retry while still idle. Emit an actionable error and failure counter. A new request cancels pending retry and revalidates readiness. No container may be treated as stopped merely because the stop API returned an error.

External exits are detected when checking readiness or handling an upstream transport failure; mark state stale and inspect before the next demand-driven start. Avoid adding another independent automatic restart loop. Docker restart policies and external controllers must not immediately restart containers intentionally stopped by this module.

## HTTP behavior and retries

### Deadline and errors

The `timeout` is a **startup/response-header deadline**, not a maximum stream duration. It includes admission waiting, Docker startup, startup delay, probes, retry waits, and upstream attempts. This makes the 10s default compatible with long downloads and WebSockets.

Use a separately cancellable timer around the proxy transport: disable it when final upstream response headers are accepted, while preserving client cancellation for the body lifetime. Simply attaching a 10s context timeout to the entire proxy request would incorrectly terminate streams after headers.

Return HTTP 500 for deadline expiry as requested. Startup/control-plane failures also return 500; normal upstream connection failures retain Caddy's proxy error behavior, generally 502. Return generic client-facing messages and log the actionable cause. Once headers are committed, a later streaming failure cannot be converted into HTTP 500.

### Retry eligibility

Retries address the gap between “container is running” and “application can successfully serve traffic.” Prefer a health endpoint when the application provides one.

* Retry only requests that had to wait for readiness on admission, including concurrent waiters adopting an initially running container. Ordinary warm traffic gets no additional retries.
* Retry transport failures and HTTP 502, 503, and 504 responses. These are the initial definition of startup-related unsuccessful responses; do not retry redirects, other 4xx responses, or every application error. Successful HTTP behavior is not restricted to 2xx.
* Retry only GET and HEAD requests without bodies. POST and other potentially side-effecting operations are forwarded once, even when `retries` is configured. Never assume that an upstream error proves no side effect occurred.
* Never retry after any response bytes have been sent to the client or after an upgrade is accepted. Retry status decisions occur inside the transport before the proxy commits headers.
* Close discarded response bodies without unbounded draining. Respect client cancellation and the remaining request deadline during backoff.
* After the retry allowance is exhausted, forward the last HTTP response unchanged. If the last attempt failed at transport level, use Caddy's normal proxy error handling. Deadline expiry still takes precedence and produces 500 before headers.

Disable Caddy's separate proxy retry loop for this directive so retries have one authoritative counter. No response buffering or arbitrary handler-chain replay is needed.

## Reload, shutdown, and operating boundaries

* Validate Docker access, container identity, upstream syntax, and shared policy before activating a configuration. Readiness itself is established on demand, since containers may intentionally be stopped.
* Preserve active leases and managers across reloads. A shared manager remains active while any referencing configuration is started. Removing its final route registration retires the manager after active requests finish; stop its container only if an owning configuration actually started.
* During Caddy shutdown, cancel startup work and drain HTTP requests through Caddy's existing shutdown lifecycle. One shared finite cleanup deadline covers lifecycle-operation waiting, request draining, and all Docker inspect/stop/reconciliation calls. Report failures without masking the original shutdown/provisioning error.
* Hard process termination cannot guarantee cleanup. The initial version does not add an external watchdog; containers may remain running until Caddy restarts and idle management resumes.
* One Caddy process owns a given container. Multiple independent Caddy instances controlling the same container require a distributed coordinator and are outside the initial scope. Fail conflicting registrations within one process; enforce deployment ownership outside it.
* Long-lived connections intentionally keep containers running. Idle application-level WebSockets do not count as idle container traffic in this version.

## Observability

Use Caddy's structured logger for startup failures, readiness timeouts, Docker errors, and failed stops. Log container identity, lifecycle transition, duration, and a bounded failure reason. Do not log incoming request URLs, query strings, health response bodies, or customer content.

Expose aggregate counters for starts, stops, failures, readiness timeouts, and proxy retries, plus startup duration and active lease/state gauges using Caddy's established metrics registration. Use bounded state/reason labels; avoid request IDs or arbitrary container identifiers as metric labels.

If administrative state inspection is needed, expose the manager snapshot through Caddy's authenticated/protected admin mechanism. A stopped container is an expected state, not a failed Caddy liveness check.

## Verification plan for implementation

Use the existing Caddy module testing patterns and a small Docker-client abstraction for deterministic lifecycle tests. Cover consequential behavior rather than isolated properties:

* Concurrent cold requests produce one start; cancelling one waiter preserves other requests.
* Readiness delay/probes consume the request budget and timeout returns 500 without leaking a lease.
* Active streaming requests prevent idle stop; release schedules it; arrival during stop restarts serially.
* Timed-out Docker operations reconcile actual state; failed stops remain observable and recover on retry.
* Cold GET retry closes discarded responses, respects the attempt budget, and eventually forwards the final response; POST and warm requests are not retried.
* Response-header timeout is disabled after headers, allowing streaming and upgraded connections to outlive 10s.
* Shared configuration, graceful reload, and partial provisioning failure retain exactly one lifecycle owner and release resources correctly.

An opt-in Docker integration scenario should use an existing test container with deliberately delayed readiness: verify cold start, simultaneous requests, the real health check, idle stop, and restart on later demand through Caddy. Include a real streaming response to validate timeout and lease behavior. No build or tests are performed as part of this design task.

## Decisions to confirm before implementation

1. Docker Engine is the first supported container runtime.
2. Idle time starts after the final request finishes, not when it arrives.
3. The timeout covers startup and response headers; it does not cut off an established stream.
4. Startup retries cover GET/HEAD transport failures and 502/503/504 responses only.
5. Explicitly configured containers may be stopped even if another process originally started them.
