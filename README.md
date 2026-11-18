# Open-Meteo caching proxy

A sharded caching proxy for Open-Meteo, built on Tarantool Cartridge.

## Quick start

For a new local cluster:

```bash
cartridge build
cartridge start -d
cartridge replicasets setup --bootstrap-vshard
```

Open http://localhost:8081 for the Cartridge admin UI.

The stateboard starts by default; see [`.cartridge.yml`](./.cartridge.yml).

## Usage

Routers expose `/forecast` for cached forecast requests and `/metrics` for metrics:

```bash
curl 'localhost:8081/forecast?latitude=56&longitude=60&current=temperature_2m'
curl localhost:8081/metrics
```

Successful (200–299), nonempty responses are cached for 60 seconds. Cache hits
include `X-Cache: HIT` and avoid upstream requests. Cache failures are logged;
requests can still succeed by fetching upstream. Expired responses are never served.

Upstream status, response bytes, and relevant headers are preserved. Redirects are
returned without being followed. Temporary failures and HTTP 429/502/503/504 may
be retried, with at most three attempts and respect for `Retry-After` within the
configured budget. Certificate validation failures are not retried.

Upstream timeouts return 504, connection/TLS failures return 502, and internal
application failures return 500. Error details are kept in server logs.

The cache is memory-only. Entries may be evicted under memory pressure and are
lost on restart or replica promotion. Cluster metadata remains persistent.

### Upstream configuration

Set environment variables before starting routers:

| Variable | Default | Purpose |
| --- | --- | --- |
| `OPEN_METEO_FORECAST_URL` | `https://api.open-meteo.com/v1/forecast` | Upstream HTTP or HTTPS endpoint |
| `OPEN_METEO_TIMEOUT_SECS` | `30` | Retry budget in positive integer seconds |
| `OPEN_METEO_HTTP_PROXY` | Unset (direct access) | Optional HTTP proxy; see below |
| `OPEN_METEO_MAX_CONCURRENT_FETCHES` | `32` | Concurrent upstream fetch limit per router (positive integer) |

For example:

```bash
export OPEN_METEO_TIMEOUT_SECS=15
```

Invalid configuration prevents router startup. Restart router processes to apply
changes. The retry budget is not a strict end-to-end deadline; requests can take
longer, and cache operations have separate timeouts.

### Upstream HTTP proxy

To send upstream requests through an HTTP proxy, set its URL before starting routers:

```bash
export OPEN_METEO_HTTP_PROXY='http://127.0.0.1:3128'
```

Use your actual proxy address and port. HTTP and HTTPS upstreams are supported,
and upstream certificates are still verified. Optional Basic authentication uses
`http://username:password@proxy-host:3128`; percent-encode special characters in
credentials. Keep credential values outside tracked configuration files.

An unset or empty value uses direct access. Standard `HTTP_PROXY`, `HTTPS_PROXY`,
`ALL_PROXY`, and `NO_PROXY` variables are not consulted; the explicit setting applies
to all forecast upstream requests. HTTPS and SOCKS proxy endpoints are unsupported.
The proxy URL must have a host and no path (apart from `/`), query, or fragment.
Proxy failures do not fall back to direct access. Proxy timeouts return 504;
authentication and protocol failures return 502. Cache hits do not contact the proxy.

### Concurrent fetch limit

When a router reaches its concurrent fetch limit, additional cache misses return
503 (`Forecast service busy`); cache hits remain available. Disconnecting a client
does not necessarily stop its fetch. This limit does not cap total memory usage.

### Request tracing and metrics

Every `/forecast` response includes `X-Request-ID`. Use it to find related router
and storage logs. Valid incoming IDs are reused; otherwise an ID is generated.
IDs are not sent upstream. Logs omit response bodies and full query strings.

The `/metrics` endpoint exposes request counts, latency, cache outcomes, upstream
attempts and retries, active fetches, and rejections. See the
[monitoring reference](monitoring/README.md) for metric names and request-ID rules.

### Grafana and Prometheus

Start the application with the quick-start commands above, then start monitoring:

```bash
docker compose -f compose.monitoring.yml up -d
```

Docker Desktop users must enable WSL integration for their distribution first.
Open [Grafana](http://localhost:3000), log in with `admin` / `admin`, and change the
password when prompted. The **Open-Meteo** folder contains the **Forecast proxy**
and **Tarantool Cartridge dashboard** dashboards, already connected to Prometheus.
Both support instance filtering.

The forecast dashboard shows request rates, statuses, p95 latency, cache hit ratio,
cache errors, upstream attempts and retries, active fetches, and rejections.
Send `/forecast` requests to populate request metrics. Metrics appear after their
first event; rate graphs need at least two scrapes. An empty panel can mean no
matching events have occurred. The cache hit ratio excludes lookup errors.

[Prometheus](http://localhost:9090) collects all five instances every 15 seconds
and retains data for 15 days. Check its **Status → Target health** page if graphs
are empty. Stopped application instances appear as unavailable targets.
The monitoring services are exposed only on localhost.

The targets in [monitoring/prometheus.yml](monitoring/prometheus.yml) use
`host.docker.internal` on ports 8081–8085. This address must reach the host running
Cartridge from inside Docker. If Docker Desktop cannot reach your WSL app,
replace that hostname in the targets with your WSL distribution's reachable IP
(find it with `hostname -I`). Ensure the app's HTTP listeners are reachable on
that interface, then restart Prometheus:

```bash
docker compose -f compose.monitoring.yml restart prometheus
```

Stop monitoring without removing its stored metrics, Grafana settings, or password:

```bash
docker compose -f compose.monitoring.yml down
```

To deliberately delete all monitoring data and reset Grafana's login:

```bash
docker compose -f compose.monitoring.yml down --volumes
```

Monitoring starts and stops independently of Cartridge. Dashboard definitions are
managed by the repository; save a copy under a new name to customize one in Grafana.

Grafana also includes provisioned alerts for unavailable instances, elevated errors
and latency, concurrency rejections, cache failures, upstream instability, and
memtx quota pressure. Open **Alerting → Alert rules** to inspect them. See the
[thresholds and response steps](monitoring/ALERTS.md). No external notifications
are configured by this setup; existing Grafana notification policies are retained.

### Known issues

**Unbounded response buffering.**
Complete upstream response bodies are buffered without a size limit.
Large responses can exhaust process memory despite the concurrent fetch limit
and cache eviction. Cache insertion remains best-effort: if a response cannot be
cached, it is still returned to the client.

**Bodies delimited by connection closure are lost.** Upstream responses without
`Content-Length` or chunked transfer encoding are returned with an empty body,
even when the upstream sends content before closing the connection.

**Informational responses replace the final response.** If an upstream sends
a 1xx response, such as `103 Early Hints`, before its final response, the proxy
returns the informational status with an empty body. The final status and body
are lost, so clients may never receive a final response.

**Cache entries are shared across upstream URLs.** Changing
`OPEN_METEO_FORECAST_URL` and restarting a router does not invalidate existing
cache entries. The same query can return a response from the previous upstream
until it expires. Routers using different upstream URLs also share cached responses
for matching queries.

## Instances configuration

Local instance configuration is stored in [instances.yml](./instances.yml) and
used by `cartridge start`.

## Topology configuration

Topology configuration is described in [`replicasets.yml`](./replicasets.yml).
It is used by `cartridge replicasets setup`.

## Tests

Unit and integration tests are in [`test`](./test).

First, we need to install test dependencies:

```bash
./deps.sh
```

Then, run linter:

```bash
.rocks/bin/luacheck .
```

Now we can run tests:

```bash
cartridge stop  # to prevent "address already in use" error
.rocks/bin/luatest -v
```

To test the Rust storage integration, build the shared library and run:

```bash
./cartridge.pre-build
.rocks/bin/luatest -v test/integration/storage_test.lua
```

To run the forecast routing and cache integration tests:

```bash
.rocks/bin/luatest -v test/integration/fetch_with_cache_test.lua
```

Building requires a C compiler, Make, and Perl. Transport tests require `openssl`
on `PATH`.

## Admin

Application has admin function [`probe`](./app/admin.lua) configured.
You can use it to probe instances:

```bash
cartridge start -d  # if you've stopped instances
cartridge admin probe \
  --name open-meteo-sharded-proxy \
  --run-dir ./tmp/run \
  --uri localhost:3302
```
