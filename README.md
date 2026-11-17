# Simple Tarantool Cartridge-based application

A sharded caching proxy for Open-Meteo, built on Tarantool Cartridge.

## Quick start

To build the application and set up the topology:

```bash
cartridge build
cartridge start -d
cartridge replicasets setup --bootstrap-vshard
```

Now you can visit http://localhost:8081 and see your application's Admin Web UI.

**Note**, that application stateboard is always started by default.
See [`.cartridge.yml`](./.cartridge.yml) file to change this behavior.

## Usage

Routers expose `/forecast` for cached forecast requests and `/metrics` for metrics:

```bash
curl 'localhost:8081/forecast?latitude=56&longitude=60&current=temperature_2m'
curl localhost:8081/metrics
```

Routers fetch forecasts from Open-Meteo and cache successful responses for 60 seconds.
Cache hits avoid upstream requests.

Upstream HTTP responses retain their status, body, and relevant headers, including
`Content-Type` and `Retry-After`. Only successful (200–299) responses with nonempty
bodies are cached. Empty responses are returned unchanged.
Bodies are preserved byte-for-byte, including non-UTF-8 and compressed data.
Upstream timeouts return a generic 504 response, connection/TLS failures return 502,
and internal application failures return 500. Detailed internal errors
are logged on the server. If storage is unavailable, the router fetches from
upstream and returns the response without requiring a successful cache write.

Caching is best-effort: cache failures are logged, valid upstream responses are
still returned, and expired responses are never served. Cache failures can increase
upstream traffic.

Forecast fetching retries temporary transport failures and HTTP 429, 502, 503, and
504, with at most three attempts. Retries respect `Retry-After` when the delay
fits within the configured budget. Certificate validation failures and other HTTP
errors are not retried. Redirect responses are returned to the client without being followed.

Expired entries are removed automatically. Under memory
pressure, least recently used entries may be evicted, including unexpired entries.
Eviction starts at 80% of the instance's configured memtx memory and targets 70%,
based on estimated live data usage. These thresholds do not cap process RAM
or guarantee that writes cannot run out of memory.

Cache contents are held only in memory and are not persisted or replicated.
Restarts and replica promotion can produce cold caches. Cluster metadata remains
persistent.

### Upstream configuration

`OPEN_METEO_TIMEOUT_SECS` sets the retry budget in positive integer seconds
(default 30). Requests can exceed this budget because connection and response
timeouts apply separately; it is not a strict end-to-end deadline.

For example, set the budget before starting router instances:

```bash
export OPEN_METEO_TIMEOUT_SECS=15
```

`OPEN_METEO_FORECAST_URL` (default `https://api.open-meteo.com/v1/forecast`) and
`OPEN_METEO_TIMEOUT_SECS` are validated when the router role starts. Invalid
configuration prevents startup. Restart router processes to apply environment
changes. Cache lookups and writes each have a one-second timeout, separate from
the upstream request budget.

### Concurrent fetch limit

Each router allows up to 32 simultaneous upstream fetches. Set
`OPEN_METEO_MAX_CONCURRENT_FETCHES` to a positive integer before starting routers
to change the limit; restart routers after changes. Cache hits remain available
when the limit is reached. Additional cache misses immediately return 503
(`Forecast service busy`), without queuing or contacting upstream.

A fetch occupies a slot through retries and cache insertion. Disconnecting a client
does not stop its ongoing fetch or immediately free its slot. The limit applies
independently to each router and does not cap total memory usage.

### Request tracing and metrics

Every `/forecast` response includes `X-Request-ID`. Valid incoming IDs are reused;
otherwise a UUID is generated. IDs must contain 1–128 ASCII letters, digits,
periods, underscores, colons, or hyphens. Repeated or invalid IDs are replaced.
The same ID identifies related router and storage logs, including upstream attempts
and retries. It is not sent to the upstream service. Cache hits return the current
request's ID, never a cached ID.

Structured log events include the request ID, instance UUID, duration, and relevant
status, bucket, attempt, and error classification. Completion summaries are logged
at info, stage details at debug, and failures at warning/error. Existing log output
settings are retained. Bodies and full queries are not included in request events.

The `/metrics` endpoint exports HTTP request counts and latency as
`http_server_request_latency_count`, `_sum`, and `_bucket`, plus:

| Metric | Meaning |
| --- | --- |
| `forecast_cache_operations_total` | Cache operations by operation and outcome |
| `forecast_upstream_attempts_total` | Upstream attempts by outcome and status |
| `forecast_upstream_attempt_duration_seconds` | Upstream attempt duration histogram |
| `forecast_upstream_retries_total` | Retries actually started, by reason |
| `forecast_active_fetches` | Fetches still occupying a concurrency slot |
| `forecast_rejected_requests_total` | Requests rejected by the concurrency limit |

Request IDs, queries, and bucket IDs are excluded from metric labels. Observations
use `status="cancelled"` when a handler is cancelled without a response. Histogram
boundaries range from 1 ms to 120 seconds, with an overflow bucket. Request tracing
does not change retry or caching behavior; disconnected callers' fetches can
continue and retain their request IDs until completion. Deploy and restart routers
and storage instances together to load these changes.

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

Configuration of instances that can be used to start application
locally is stored in [instances.yml](./instances.yml).
It is used by `cartridge start`.

## Topology configuration

Topology configuration is described in [`replicasets.yml`](./replicasets.yml).
It is used by `cartridge replicasets setup`.

## Tests

Simple unit and integration tests are placed in [`test`](./test) directory.

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
on `PATH`. The slow-upstream timeout test exercises the default 30-second retry budget.

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
