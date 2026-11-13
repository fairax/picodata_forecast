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

## Application

The application entry point is [`init.lua`](./init.lua).

## Roles

The router role, [`app.roles.custom`](./app/roles/custom.lua), exposes `/forecast` for cached forecast requests and `/metrics` for metrics:

```bash
curl 'localhost:8081/forecast?latitude=56&longitude=60&current=temperature_2m'
curl localhost:8081/metrics
```

Routers fetch forecasts from Open-Meteo and cache successful responses on the
storage shard responsible for the normalized query. Cache hits avoid upstream
requests.

Upstream HTTP responses retain their status, body, and relevant headers, including
`Content-Type` and `Retry-After`. Only successful (200–299) responses are cached.
Upstream timeouts return a generic 504 response, connection/TLS failures return 502,
and internal application failures return 500. Detailed internal errors
are logged on the server. If storage is unavailable, the router fetches from
upstream and returns the response without requiring a successful cache write.

Caching is best-effort: cache failures are logged, valid upstream responses are
still returned, and expired responses are never served. Cache failures can increase
upstream traffic.

Forecast fetching retries temporary transport failures and HTTP 429, 502, 503, and
504 up to three attempts within `OPEN_METEO_TIMEOUT_SECS` (default 30 seconds).
Each attempt is capped at 10 seconds. Retries respect `Retry-After` when the delay
fits within the remaining budget. Certificate validation failures and other HTTP
errors are not retried.

Expired entries are removed automatically on writable storage masters. Under memory
pressure, least recently used entries may be evicted, including unexpired entries.
Eviction starts at 80% of the instance's configured memtx memory and targets 70%,
measured using live tuple and index bytes. These thresholds do not cap process RAM
or guarantee that writes cannot run out of memory.

Cache contents are held only in memory and are not persisted or replicated.
Restarts and replica promotion can produce cold caches. Cluster metadata remains
persistent. On upgrade, a legacy persistent cache is recreated as an in-memory
cache, discarding its contents; historical WAL and snapshot files are not erased.
Deploy the native library and roles together and restart routers and storage
instances together; mixed-version operation is not supported.

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

A fetch occupies a slot through retries and cache insertion. Cancellation does not
free its slot until the native worker finishes. The limit applies independently
to each router and bounds concurrent fetches, not total RAM or responses buffered
for clients. The response body size limit still applies.

### Upstream body size limit

`OPEN_METEO_MAX_RESPONSE_BYTES` limits upstream response bodies to 1 MiB by default.
Set a positive integer byte count before starting router instances, for example:

```bash
export OPEN_METEO_MAX_RESPONSE_BYTES=2097152
```

Restart router instances after changes. The limit also applies after UTF-8 text
conversion. Oversized responses return 502 (`Forecast response too large`) and are
neither retried nor cached.

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

Transport tests require `openssl` on `PATH`. The timeout test uses the full
30-second request budget.

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
