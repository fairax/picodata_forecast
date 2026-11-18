# Local HTTP proxy patch

Base: crates.io `fibreq` **0.2.0**, registry checksum
`abd37d3d58d46f56aea9630f7c5a720937a69fcb913935679df5989f76bdb019`.
Upstream: https://git.picodata.io/picodata/mod/fibreq
License: BSD-2-Clause; the upstream license is retained in `LICENSE`.

The parent Cargo manifest uses `[patch.crates-io]` to select this copy. The
Tarantool SDK remains 6.1; the direct connection implementation and HTTP decoder
are unchanged. This does not fix the HTTP parsing limitations in the main README.

Changes:

- Export `Proxy::http()` and `ClientBuilder::proxy()`; proxy routing is opt-in.
- HTTP uses an absolute-form request target with async-h1's original header/body
  encoder and response decoder. HTTPS performs bounded CONNECT parsing, then TLS
  with the origin hostname for SNI and certificate verification.
- Basic credentials are percent-decoded once, omitted from Debug/error output,
  and sent only as proxy authentication. HTTPS origin requests never receive them.
- Proxied exchanges use a private TCP socket, without pooling or direct fallback.
  TCP and async socket operations use Tarantool's existing cooperative I/O. The
  exchange deadline covers CONNECT, TLS, request writing, and response headers;
  body reading retains fibreq's existing response timeout.
- Add typed configuration, protocol and CONNECT-status errors. The application
  retries temporary CONNECT statuses under its existing attempt/budget limits.
- Add direct dependencies on already-locked `futures-lite` and `percent-encoding`.

Only `src/proxy.rs`, `src/client.rs`, `src/error.rs`, `src/lib.rs`, and `Cargo.toml`
differ from the published source. Proxy integration tests live in
`test/integration/fetch_with_cache_test.lua`; run with `-p test_proxy`.
