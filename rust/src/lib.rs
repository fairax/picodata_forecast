use tarantool::proc;

mod forecast;
mod observability;
mod storage;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Validate and save forecast environment settings during router role initialization.
/// Repeated calls preserve the existing configuration; invalid settings fail startup.
#[proc]
fn init_forecast() -> Result<()> {
    forecast::initialize()
        .map_err(|error| format!("Invalid forecast configuration: {error:?}").into())
}

/// Create the in-memory forecast cache space and its indexes if absent.
/// Called during storage master initialization; existing cache contents are preserved.
#[proc]
fn create_schema() -> tarantool::Result<()> {
    storage::create_schema()
}

/// Scan at most 100 expired cache entries without yielding, deleting available ones.
/// Resume after an optional `(expires_at, key)` cursor and return counts plus continuation.
/// The background driver yields between batches and resets the cursor after a sweep.
#[proc]
fn cleanup_expired(cursor: Option<storage::CleanupCursor>) -> Result<storage::CleanupBatch> {
    storage::cleanup_expired(cursor)
}

/// Evict oldest-accessed cache entries until live memtx usage reaches `target_bytes`.
/// Scan at most 100 entries without yielding, skipping unavailable buckets.
/// Return counts and continuation using an independent `(last_access, key)` cursor.
#[proc]
fn evict_cache(
    cursor: Option<storage::CleanupCursor>,
    target_bytes: u64,
) -> Result<storage::CleanupBatch> {
    storage::evict_cache(cursor, target_bytes)
}

/// Canonicalize query encoding and stably sort parameters by name.
/// Repeated parameters retain their value order so equivalent queries share a cache key.
#[proc]
fn normalize_query(query: &str) -> String {
    forecast::NormalizedQuery::new(query).to_string()
}

/// Fetch a normalized forecast query with retries, yielding during HTTP I/O and backoff.
/// Bypass the cache and preserve upstream statuses, body bytes and repeated headers.
/// Log failures with `request_id` and return generic HTTP error responses.
#[proc]
fn fetch_forecast(query: &str, request_id: &str) -> Result<forecast::Response> {
    let normalized_query = forecast::NormalizedQuery::new(query);
    Ok(
        forecast::fetch(&normalized_query, request_id).unwrap_or_else(|error| {
            observability::event(
                "error",
                "forecast_failure",
                request_id,
                "",
                error.kind(),
                0,
                0,
                0.0,
                0,
                error.detail(),
            );
            forecast::Response::from(error)
        }),
    )
}

/// Look up a normalized query and return a fresh cached response with `X-Cache: HIT`.
/// Expired entries are misses; deletion and access-time update failures are logged.
/// Report lookup/decoding errors to the caller and record the outcome with `request_id`.
#[proc]
fn cache_get(query: &str, request_id: &str) -> Result<Option<forecast::Response>> {
    let started = std::time::Instant::now();
    let result = storage::get_cache_entry(&forecast::NormalizedQuery::new(query), request_id);
    let outcome = match &result {
        Ok(Some(_)) => "hit",
        Ok(None) => "miss",
        Err(_) => "error",
    };
    let detail = result
        .as_ref()
        .err()
        .map(ToString::to_string)
        .unwrap_or_default();
    observability::event(
        if result.is_err() { "warn" } else { "debug" },
        "storage_cache_get",
        request_id,
        "get",
        outcome,
        0,
        0,
        started.elapsed().as_secs_f64(),
        0,
        &detail,
    );
    result
}

/// Cache a nonempty 2xx response for 60 seconds under its normalized query and bucket.
/// Strip response request IDs; other responses are skipped. Return true on success or skip.
/// Log the outcome with `request_id` and propagate insertion errors to the caller.
#[proc]
fn cache_put(
    query: &str,
    bucket_id: u64,
    response: forecast::Response,
    request_id: &str,
) -> Result<bool> {
    let started = std::time::Instant::now();
    let mut response = response;
    response
        .headers
        .retain(|name, _| !name.eq_ignore_ascii_case("x-request-id"));
    if (200..300).contains(&response.status) && !response.body.is_empty() {
        let result = storage::add_cache_entry(
            forecast::NormalizedQuery::new(query).to_string(),
            bucket_id,
            &response,
            std::time::Duration::from_secs(60),
        );
        let detail = result
            .as_ref()
            .err()
            .map(ToString::to_string)
            .unwrap_or_default();
        observability::event(
            if result.is_err() { "warn" } else { "debug" },
            "storage_cache_put",
            request_id,
            "put",
            if result.is_err() { "error" } else { "success" },
            0,
            0,
            started.elapsed().as_secs_f64(),
            bucket_id,
            &detail,
        );
        result?;
    } else {
        observability::event(
            "debug",
            "storage_cache_put",
            request_id,
            "put",
            "skipped",
            0,
            0,
            started.elapsed().as_secs_f64(),
            bucket_id,
            "",
        );
    }
    Ok(true)
}
