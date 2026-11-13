use std::sync::{Arc, Mutex};

use tarantool::proc;

mod forecast;
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
fn fetch_forecast(query: &str) -> Result<forecast::Response> {
    use forecast::FetchError;

    let result = (|| -> std::result::Result<forecast::Response, FetchError> {
        let normalized_query = forecast::NormalizedQuery::new(query);
        let output = Arc::new(Mutex::new(None));
        let worker_output = Arc::clone(&output);
        // Only owned query/response data crosses threads; the worker makes no Tarantool calls.
        let mut worker = move |query: Box<forecast::NormalizedQuery>| {
            let result = std::panic::catch_unwind(|| forecast::fetch(&query))
                .unwrap_or_else(|_| Err(FetchError::internal("Forecast HTTP worker panicked")));
            let Ok(mut output) = worker_output.lock() else {
                return 0;
            };
            *output = Some(result);
            0
        };
        let rc = tarantool::coio::coio_call(&mut worker, normalized_query);
        if tarantool::fiber::is_cancelled() {
            return Err(FetchError::internal("Forecast request cancelled"));
        }
        if rc == -1 {
            return Err(FetchError::internal(std::io::Error::last_os_error()));
        }
        let response = output
            .lock()
            .map_err(|_| FetchError::internal("Forecast result mutex poisoned"))?
            .take()
            .ok_or_else(|| FetchError::internal("Forecast HTTP worker returned no result"))??;
        Ok(response)
    })();
    Ok(result.unwrap_or_else(forecast::Response::from))
}

#[proc]
fn cache_get(query: &str) -> Result<Option<forecast::Response>> {
    storage::get_cache_entry(&forecast::NormalizedQuery::new(query))
}

#[proc]
fn cache_put(query: &str, bucket_id: u64, response: forecast::Response) -> Result<bool> {
    if (200..300).contains(&response.status) {
        storage::add_cache_entry(
            forecast::NormalizedQuery::new(query).to_string(),
            bucket_id,
            &response,
            std::time::Duration::from_secs(60),
        )?;
    }
    Ok(true)
}
