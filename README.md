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
502. No application-level retries are performed.

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
