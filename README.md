# caddy-cod: demand-driven Docker containers

`container_proxy` starts an existing Docker container when a request needs it,
proxies through Caddy's `reverse_proxy` implementation, and stops the container
after it has been idle. It does not create, remove, or update containers.

## Build and install

Build Caddy with this module using [xcaddy](https://github.com/caddyserver/xcaddy):

```sh
xcaddy build --with github.com/charlesfeval/caddy-cod=.
```

Run the resulting Caddy binary with the Docker environment configured. The
module uses Docker's Go client settings, including `DOCKER_HOST`,
`DOCKER_TLS_VERIFY`, and `DOCKER_CERT_PATH`, and negotiates the Engine API
version. The Caddy process needs permission to inspect, start, and stop the
configured containers. Docker socket access is privileged infrastructure
access; do not expose it through a public endpoint.

## Caddyfile

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
```

The upstream is a single HTTP or HTTPS origin reachable from Caddy, such as a
published host port or a stable DNS name on a shared Docker network. It must
not include a path, credentials, query, or fragment. Caddy proxies the incoming
request path and query to this origin. Health paths are relative to the origin
and independent of the incoming request.

| Option | Required / default | Meaning |
| --- | --- | --- |
| `container` | Required | Existing container name or ID. It is resolved to its Docker ID when Caddy provisions the route. |
| `idle_timeout` | Required | Positive Go duration after the last active request before Docker stop is requested. |
| `timeout` | `10s` | Positive Go duration covering admission, startup/readiness, retries, and upstream response headers. It does not limit the response stream after headers. |
| `health_endpoint` | Optional | Absolute path checked until it returns HTTP 2xx. Without it, Docker's running state is sufficient. |
| `startup_delay` | `0s` | Nonnegative Go duration after Docker reports running and before readiness/proxying. |
| `retries` | `0` | Maximum extra attempts for cold GET/HEAD requests without a body. Retries cover transport failures and 502/503/504 only. |
| `retry_backoff` | `250ms` | Positive fixed delay between retries when retries are enabled. |

Containers already running when the route is activated are adopted and receive
an idle timer. A stopped container remains stopped until a request arrives.
Configuring a container opts Caddy into stopping it. Ensure Docker restart
policies or external controllers will not immediately undo an intentional
idle stop. Use `route` if this directive must be ordered explicitly with other
handlers.

## Runtime behavior and current limits

Routes in one Caddy process that resolve to the same Docker endpoint and
container ID share one lifecycle owner. They must agree on idle timeout,
startup delay, upstream origin, and health endpoint; request timeouts and retry
settings can differ. Reloads share that owner while old and new configurations
overlap. Removing its final reference stops the managed container after
active requests drain.

Each request has its own deadline; a canceled waiter does not cancel startup
while other request leases remain. A shared startup attempt has a fixed
two-minute upper bound, independent of per-route request timeouts. Stop uses a
10-second Docker grace period and retries a failed idle stop once per minute
while the container remains idle. A finite 30-second cleanup drain prevents
shutdown from hanging forever; if requests do not drain, cleanup logs the
condition and leaves the container running.

The readiness probe uses Go's standard HTTP transport and system trust roots;
the Caddy reverse proxy uses its provisioned HTTP transport. This version does
not expose custom per-route reverse-proxy transport settings, aggregate Caddy
metrics, or an administrative lifecycle-state endpoint. Docker API failures,
startup/readiness failures, and failed stops are logged without request URLs
or health response bodies.

Only one Caddy process should manage a given container. Coordinating multiple
independent Caddy processes requires an external ownership mechanism.
