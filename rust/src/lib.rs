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
    let normalized_query = forecast::NormalizedQuery::new(query);
    Ok(forecast::fetch(&normalized_query).unwrap_or_else(forecast::Response::from))
}

#[proc]
fn cache_get(query: &str) -> Result<Option<forecast::Response>> {
    storage::get_cache_entry(&forecast::NormalizedQuery::new(query))
}

#[proc]
fn cache_put(query: &str, bucket_id: u64, response: forecast::Response) -> Result<bool> {
    if (200..300).contains(&response.status) && !response.body.is_empty() {
        storage::add_cache_entry(
            forecast::NormalizedQuery::new(query).to_string(),
            bucket_id,
            &response,
            std::time::Duration::from_secs(60),
        )?;
    }
    Ok(true)
}
