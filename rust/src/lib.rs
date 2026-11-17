use tarantool::proc;

mod forecast;
mod observability;
mod storage;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[proc]
fn init_forecast() -> Result<()> {
    forecast::initialize()
        .map_err(|error| format!("Invalid forecast configuration: {error:?}").into())
}

#[proc]
fn create_schema() -> tarantool::Result<()> {
    storage::create_schema()
}

#[proc]
fn cleanup_expired(cursor: Option<storage::CleanupCursor>) -> Result<storage::CleanupBatch> {
    storage::cleanup_expired(cursor)
}

#[proc]
fn evict_cache(
    cursor: Option<storage::CleanupCursor>,
    target_bytes: u64,
) -> Result<storage::CleanupBatch> {
    storage::evict_cache(cursor, target_bytes)
}

#[proc]
fn normalize_query(query: &str) -> String {
    forecast::NormalizedQuery::new(query).to_string()
}

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
