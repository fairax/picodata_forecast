use serde_bytes::ByteBuf;
use tarantool::error::{BoxError, TarantoolErrorCode};
use tarantool::index::IndexOptions;
use tarantool::space::{Field, Space};

const CACHE_SPACE: &str = "forecast_cache";

#[tarantool::proc]
pub fn storage_init() -> tarantool::Result<bool> {
    tarantool::say_info!("Initializing forecast cache schema");
    create_schema()?;
    tarantool::say_info!("Forecast cache schema is ready");
    Ok(true)
}

fn create_schema() -> tarantool::Result<()> {
    let space = Space::builder(CACHE_SPACE)
        .if_not_exists(true)
        .format([
            Field::string("key"),
            Field::unsigned("bucket_id"),
            Field::varbinary("body"),
            Field::unsigned("expires_at"),
            Field::unsigned("last_access"),
        ])
        .create()?;

    space.create_index(
        "primary",
        &IndexOptions {
            if_not_exists: Some(true),
            parts: Some(vec!["key".into()]),
            ..Default::default()
        },
    )?;

    space.create_index(
        "bucket_id",
        &IndexOptions {
            unique: Some(false),
            if_not_exists: Some(true),
            parts: Some(vec!["bucket_id".into()]),
            ..Default::default()
        },
    )?;

    space.create_index(
        "expires_at",
        &IndexOptions {
            unique: Some(false),
            if_not_exists: Some(true),
            parts: Some(vec!["expires_at".into(), "key".into()]),
            ..Default::default()
        },
    )?;

    space.create_index(
        "lru",
        &IndexOptions {
            unique: Some(false),
            if_not_exists: Some(true),
            parts: Some(vec!["last_access".into(), "key".into()]),
            ..Default::default()
        },
    )?;

    Ok(())
}

#[derive(serde::Deserialize)]
pub struct CacheRecord {
    pub key: String,
    pub bucket_id: u64,
    pub body: ByteBuf,
    pub expires_at: u64,
    pub last_access: u64,
}

fn cache_space() -> tarantool::Result<Space> {
    Space::find(CACHE_SPACE).ok_or_else(|| {
        BoxError::new(
            TarantoolErrorCode::NoSuchSpace,
            format!("Space '{CACHE_SPACE}' does not exist"),
        )
        .into()
    })
}

pub fn get_cache_entry(key: &str) -> tarantool::Result<Option<CacheRecord>> {
    let Some(tuple) = cache_space()?.get(&(key,))? else {
        return Ok(None);
    };
    Ok(Some(tuple.decode()?))
}

pub fn put_cache_entry(
    key: String,
    bucket_id: u64,
    body: ByteBuf,
    expires_at: u64,
    last_access: u64,
) -> tarantool::Result<()> {
    cache_space()?.replace(&(key, bucket_id, body, expires_at, last_access))?;

    Ok(())
}
