use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tarantool::space::UpdateOps;
use tarantool::tuple::Tuple;
use tarantool::{
    index::{FieldType as IndexField, IteratorType},
    space::{Field, Space},
};

use crate::{Result, forecast::Response};

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

fn forecast_cache() -> Result<Space> {
    Space::find("forecast_cache").ok_or_else(|| "Space 'forecast_cache' not found".into())
}

fn expires_at_index() -> Result<tarantool::index::Index> {
    forecast_cache()?
        .index("expires_at")
        .ok_or("Missing cache index: expires_at".into())
}

fn expire() -> Result<Vec<String>> {
    let index = expires_at_index()?;
    let timestamp = now();
    let expired_entries: Vec<String> = index
        .select(IteratorType::LE, &(timestamp,))?
        .map(|tuple| CacheEntry(&tuple).key())
        .collect::<Result<Vec<_>>>()?;

    let forecast_cache = forecast_cache()?;
    for key in &expired_entries {
        forecast_cache.delete(&(key,))?;
    }
    Ok(expired_entries)
}

pub struct CacheEntry<'a>(pub &'a Tuple);

impl CacheEntry<'_> {
    fn field<T: serde::de::DeserializeOwned>(&self, position: u32, name: &str) -> Result<T> {
        self.0
            .field(position)?
            .ok_or_else(|| format!("Missing cache field: {name}").into())
    }

    pub fn key(&self) -> Result<String> {
        self.field(0, "key")
    }

    pub fn status(&self) -> Result<u16> {
        self.field(2, "status")
    }

    pub fn body(&self) -> Result<String> {
        self.field(3, "body")
    }

    pub fn headers(&self) -> Result<BTreeMap<String, String>> {
        self.field(4, "headers")
    }

    pub fn expires_at(&self) -> Result<u64> {
        self.field(5, "expires_at")
    }
}

pub fn create_schema() -> tarantool::Result<()> {
    let space = Space::builder("forecast_cache")
        .if_not_exists(true)
        .format([
            Field::string("key"),
            Field::unsigned("bucket_id"),
            Field::unsigned("status"),
            Field::string("body"),
            Field::map("headers"),
            Field::unsigned("expires_at"),
            Field::unsigned("last_access"),
        ])
        .create()?;

    space
        .index_builder("primary")
        .id(0)
        .unique(true)
        .parts([("key", IndexField::String)])
        .if_not_exists(true)
        .create()?;

    space
        .index_builder("bucket_id")
        .id(1)
        .unique(false)
        .parts([("bucket_id", IndexField::Unsigned)])
        .if_not_exists(true)
        .create()?;

    space
        .index_builder("expires_at")
        .id(2)
        .unique(false)
        .parts([
            ("expires_at", IndexField::Unsigned),
            ("key", IndexField::String),
        ])
        .if_not_exists(true)
        .create()?;

    space
        .index_builder("lru")
        .id(3)
        .unique(false)
        .parts([
            ("last_access", IndexField::Unsigned),
            ("key", IndexField::String),
        ])
        .if_not_exists(true)
        .create()?;

    Ok(())
}

pub fn add_cache_entry(
    key: String,
    bucket_id: u64,
    response: &Response,
    ttl: Duration,
) -> Result<()> {
    expire()?;
    let forecast_cache = forecast_cache()?;
    let timestamp = now();
    forecast_cache.replace(&(
        key,
        bucket_id,
        response.status,
        &response.body,
        &response.headers,
        timestamp.saturating_add(ttl.as_micros() as u64),
        timestamp,
    ))?;

    Ok(())
}

pub fn get_cache_entry(key: &str) -> Result<Option<Response>> {
    let forecast_cache = forecast_cache()?;
    let Some(tuple) = forecast_cache.get(&(key,))? else {
        return Ok(None);
    };
    let entry = CacheEntry(&tuple);
    let timestamp = now();
    if entry.expires_at()? <= timestamp {
        forecast_cache.delete(&(key,))?;
        return Ok(None);
    }
    let mut result: Response = (&tuple).try_into()?;
    let mut ops = UpdateOps::new();
    ops.assign(6, timestamp)?;
    forecast_cache.update(&(key,), ops)?;
    result.headers.insert("x-cache".into(), "HIT".into());
    Ok(Some(result))
}
