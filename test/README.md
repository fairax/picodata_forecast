# Application tests

Build the native library with `./cartridge.pre-build` before running integration
tests. Tests use temporary data directories and separate ports; the development
cluster can keep running. Transport fixtures require `openssl` on `PATH`.

```bash
# Fast checks without a Cartridge cluster
.rocks/bin/luatest -v test/unit test/integration/configuration_test.lua

# Complete application suite
.rocks/bin/luatest -v
.rocks/bin/luacheck test
```

Pass an individual test file to `luatest` to run one suite.

| Suite | Coverage | Fixture |
| --- | --- | --- |
| `unit/observability_test.lua` | Request ID validation; HTTP wrapper results, errors, cancellation and metric failures | Local functions |
| `integration/configuration_test.lua` | Defaults, accepted values, invalid configuration and credential redaction | Fresh processes |
| `integration/transport_test.lua` | Retry limits and delays, timeouts, TLS and incomplete HTTP bodies | One instance and mock upstream |
| `integration/proxy_test.lua` | HTTP proxy authentication, CONNECT, TLS, failures and timeouts | Sharded cluster and mock proxy |
| `integration/router_test.lua` | HTTP forwarding, binary bodies, headers, cache fallbacks, concurrency limits, routing and bucket transfer | Sharded cluster and mock upstream |
| `integration/observability_test.lua` | Request correlation, structured events, metrics export and bounded labels | Sharded cluster and mock upstream |
| `integration/storage_test.lua` | Schema, storage operations, expiration on read, temporary contents and promotion | Storage instances |
| `integration/cleanup_test.lua` | Bounded expiration and eviction, lifecycle, bucket references and fiber progress | Storage instances |

The former `fetch_with_cache_test.lua` coverage is divided among configuration,
transport, proxy, router and observability suites. Shared fixtures live in
`test/helpers/`; protocol fault servers live in `test/fixtures/`.

Template API and unit tests were removed. Short timeout tests replace the default
30-second wait. Cancellation checks share one end-to-end case; incomplete bodies
use fixed-length and chunked cases plus a successful retry/cache case. Tests tied
to scheduler busy loops, duplicate backoff budgets or insertion-triggered cleanup
were removed. Expiration-on-read tests pause background cleanup so each behavior
is checked independently. Monitoring alert tests remain separate and unchanged.
