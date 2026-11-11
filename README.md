# Simple Tarantool Cartridge-based application

This a simplest application based on Tarantool Cartridge.

## Quick start

To build application and setup topology:

```bash
cartridge build
cartridge start -d
cartridge replicasets setup --bootstrap-vshard
```

Now you can visit http://localhost:8081 and see your application's Admin Web UI.

**Note**, that application stateboard is always started by default.
See [`.cartridge.yml`](./.cartridge.yml) file to change this behavior.

## Application

Application entry point is [`init.lua`](./init.lua) file.
It configures Cartridge, initializes admin functions and exposes metrics endpoints.
Before requiring `cartridge` module `package_compat.cfg()` is called.
It configures package search path to correctly start application on production
(e.g. using `systemd`).

## Roles

Application has one simple role, [`app.roles.custom`](./app/roles/custom.lua).
It exposes `/forecast` for cached forecast requests and `/metrics` for metrics:

```bash
curl 'localhost:8081/forecast?latitude=56&longitude=60&current=temperature_2m'
curl localhost:8081/metrics
```

The router normalizes forecast query parameters, computes a vshard bucket with
`bucket_id_strcrc32`, and calls `forecast.fetch_with_cache` on that bucket's owner.
The stored `bucket_id` is the routing bucket, so cached tuples move with their bucket.
The low-level Rust procedure takes `(query, bucket_id)`; use `/forecast` for normal
requests so vshard validates ownership and holds the bucket while the request runs.

Upstream HTTP responses retain their status, body, and relevant headers, including
`Content-Type` and `Retry-After`. Only successful (200–299) responses are cached.
Upstream timeouts return a generic 504 response, connection/TLS failures return 502,
and internal application or storage failures return 500. Detailed internal errors
are logged on the server; router-to-storage communication failures return a generic
502.

Forecast fetching allows up to three attempts for temporary transport failures and
HTTP 429, 502, 503, and 504. All attempts and delays share a budget configured by
`OPEN_METEO_TIMEOUT_SECS` (positive integer seconds, default 30), with
at most 10 seconds per attempt. Retries use jittered backoff (up to 200 ms, then
400 ms) and respect `Retry-After` seconds or HTTP dates. If that delay exceeds the
remaining budget, the upstream response is returned immediately. Other HTTP errors,
invalid configuration, and certificate validation failures are not retried. The last
upstream HTTP response is preserved when retries are exhausted; only a successful
response is cached. Concurrent requests are not coalesced.

For example, set the budget before starting the instances:

```bash
export OPEN_METEO_TIMEOUT_SECS=15
```

`OPEN_METEO_FORECAST_URL` (default `https://api.open-meteo.com/v1/forecast`) and
`OPEN_METEO_TIMEOUT_SECS` are read and validated once when the storage role starts,
on both masters and replicas. Invalid URLs and empty, invalid, zero, or overflowing
timeouts fail role initialization. Requests reuse the saved configuration; restart
the storage processes to apply environment changes. The router's RPC timeout stays
fixed at 35 seconds, so longer HTTP budgets can
outlive the router's wait. The three-attempt limit and 10-second per-attempt cap
still apply.

Also, Cartridge roles [are registered](./init.lua)
(`vshard-storage`, `vshard-router` and `metrics`).

You can add your own role, but don't forget to register in using
`cartridge.cfg` call.

## Instances configuration

Configuration of instances that can be used to start application
locally is places in [instances.yml](./instances.yml).
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

These tests start a temporary Cartridge storage instance and exercise the compiled
Rust procedures through `net.box`. They cover schema initialization, cache reads,
refreshes, missing keys, and expiration. They do not test vshard routing or replication.

The forecast integration tests additionally start a router and two storage shards,
checking normalized-query routing, tuple bucket ownership, and cached reads after a
bucket transfer:

```bash
.rocks/bin/luatest -v test/integration/fetch_with_cache_test.lua
```

The transport fault tests run a Lua fixture with Tarantool. The certificate test
requires `openssl` on `PATH` to generate a temporary untrusted certificate and uses
Tarantool's exported OpenSSL APIs through LuaJIT FFI. All fault servers listen on
localhost only.
The timeout test exercises three attempts within the real 30-second budget.

Configuration tests restart test storage processes for new settings and use isolated
processes to verify startup validation, including replicas and environment defaults.

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
