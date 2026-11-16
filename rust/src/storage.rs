use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_bytes::ByteBuf;
use tarantool::space::UpdateOps;
use tarantool::tuple::Tuple;
use tarantool::{
    index::{FieldType as IndexField, IteratorType},
    space::{Field, Space, SpaceType},
};

use crate::{Result, forecast::Response};

pub fn create_schema() -> tarantool::Result<()> {
    let space = Space::builder("forecast_cache")
        .space_type(SpaceType::DataTemporary)
        .if_not_exists(true)
        .format([
            Field::string("key"),
            Field::unsigned("bucket_id"),
            Field::unsigned("status"),
            Field::varbinary("body"),
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
        if let Err(error) = forecast_cache.delete(&(key,)) {
            tarantool::say_error!("Expired cache entry deletion failed: {}", error);
        }
        return Ok(None);
    }
    let mut result: Response = (&tuple).try_into()?;
    let touch = (|| -> Result<()> {
        let mut ops = UpdateOps::new();
        ops.assign(6, timestamp)?;
        forecast_cache.update(&(key,), ops)?;
        Ok(())
    })();
    if let Err(error) = touch {
        tarantool::say_error!("Cache access time update failed: {}", error);
    }
    result.headers.insert("x-cache".into(), vec!["HIT".into()]);
    Ok(Some(result))
}

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

fn forecast_cache() -> Result<Space> {
    Space::find("forecast_cache").ok_or_else(|| "Space 'forecast_cache' not found".into())
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

    pub fn bucket_id(&self) -> Result<u64> {
        self.field(1, "bucket_id")
    }

    pub fn status(&self) -> Result<u16> {
        self.field(2, "status")
    }

    pub fn body(&self) -> Result<ByteBuf> {
        self.field(3, "body")
    }

    pub fn headers(&self) -> Result<BTreeMap<String, Vec<String>>> {
        self.field(4, "headers")
    }

    pub fn expires_at(&self) -> Result<u64> {
        self.field(5, "expires_at")
    }
}

pub type CleanupCursor = (u64, String);

#[derive(serde::Serialize)]
pub struct CleanupBatch {
    pub scanned: usize,
    pub deleted: usize,
    pub next_cursor: Option<CleanupCursor>,
}

// All bucket reference operations run on the Tarantool thread.
struct BucketRef(Option<u64>);

impl BucketRef {
    fn acquire(bucket_id: u64, sharded: bool) -> Result<Option<Self>> {
        if !sharded {
            return Ok(Some(Self(None)));
        }
        let acquired: bool = tarantool::lua_state().eval_with(
            "local id = ...; local ok = require('vshard').storage.bucket_refrw(id); return ok == true",
            bucket_id,
        ).map_err(tarantool::tlua::LuaError::from)?;
        Ok(if acquired {
            Some(Self(Some(bucket_id)))
        } else {
            None
        })
    }

    fn release(mut self) -> Result<()> {
        if let Some(bucket_id) = self.0.take() {
            Self::release_bucket(bucket_id)?;
        }
        Ok(())
    }

    fn release_bucket(bucket_id: u64) -> Result<()> {
        tarantool::lua_state().exec_with(
            "local id = ...; local ok, err = require('vshard').storage.bucket_unrefrw(id); assert(ok, tostring(err))",
            bucket_id,
        ).map_err(tarantool::tlua::LuaError::from)?;
        Ok(())
    }
}

impl Drop for BucketRef {
    fn drop(&mut self) {
        if let Some(bucket_id) = self.0.take() {
            if let Err(error) = Self::release_bucket(bucket_id) {
                tarantool::say_error!("Could not release cleanup bucket reference: {}", error);
            }
        }
    }
}

/// Examine at most 100 expired rows in index order without yielding.
/// The caller yields between batches and restarts the sweep after next_cursor=None.
pub fn cleanup_expired(cursor: Option<CleanupCursor>) -> Result<CleanupBatch> {
    let mut batch = CleanupBatch {
        scanned: 0,
        deleted: 0,
        next_cursor: None,
    };
    let lua = tarantool::lua_state();
    let writable: bool = lua.eval("return not box.info.ro")?;
    if !writable {
        return Ok(batch);
    }
    let space = forecast_cache()?;
    let index = space
        .index("expires_at")
        .ok_or("Missing cache index: expires_at")?;
    let cutoff = now();
    let iterator = match cursor {
        Some(cursor) => index.select(IteratorType::GT, &cursor)?,
        None => index.select(IteratorType::GE, &())?,
    };
    let sharded: bool = lua.eval("return box.space._bucket ~= nil")?;
    // Deleting the current tuple is safe during traversal. Keep this loop non-yielding.
    for tuple in iterator.take(100) {
        let entry = CacheEntry(&tuple);
        let expires_at = entry.expires_at()?;
        if expires_at > cutoff {
            break;
        }
        let key = entry.key()?;
        let bucket_id = entry.bucket_id()?;
        batch.scanned += 1;
        // Advance even when this bucket is unavailable, avoiding starvation.
        if batch.scanned == 100 {
            batch.next_cursor = Some((expires_at, key.clone()));
        }
        let Some(reference) = BucketRef::acquire(bucket_id, sharded)? else {
            continue; // Advance past locked/moving buckets; revisit on the next sweep.
        };
        let deletion = space.delete(&(key.as_str(),));
        // Release even when deleting the candidate failed.
        reference.release()?;
        let deleted = match deletion {
            Ok(tuple) => tuple.is_some(),
            Err(error) => {
                // Demotion may happen while a write yields; stop quietly then.
                if lua.eval::<bool>("return box.info.ro")? {
                    break;
                }
                return Err(error.into());
            }
        };
        batch.deleted += usize::from(deleted);
    }
    Ok(batch)
}

/// Evict up to 100 LRU candidates without yielding, until live usage reaches target.
/// This cursor uses (last_access, key), independently of the expiration cursor.
pub fn evict_cache(cursor: Option<CleanupCursor>, target_bytes: u64) -> Result<CleanupBatch> {
    let mut batch = CleanupBatch {
        scanned: 0,
        deleted: 0,
        next_cursor: None,
    };
    let lua = tarantool::lua_state();
    if lua.eval::<bool>("return box.info.ro")? {
        return Ok(batch);
    }
    let space = forecast_cache()?;
    let index = space.index("lru").ok_or("Missing cache index: lru")?;
    let mut iterator = match cursor {
        Some(cursor) => index.select(IteratorType::GT, &cursor)?,
        None => index.select(IteratorType::GE, &())?,
    };
    let sharded: bool = lua.eval("return box.space._bucket ~= nil")?;
    while batch.scanned < 100 {
        // box.info.memory() yields to collect network stats. These slab fields
        // give the same live memtx tuple/index bytes without crossing threads.
        let used: u64 = lua.eval(
            "local s = box.slab.info(); return s.arena_used + \
             (box.cfg.memtx_allocator == 'system' and s.items_used or 0)",
        )?;
        if used <= target_bytes {
            break;
        }
        let Some(tuple) = iterator.next() else {
            break;
        };
        let entry = CacheEntry(&tuple);
        let key = entry.key()?;
        let bucket_id = entry.bucket_id()?;
        let last_access: u64 = entry.field(6, "last_access")?;
        batch.scanned += 1;
        if batch.scanned == 100 {
            batch.next_cursor = Some((last_access, key.clone()));
        }
        // Do not pin deleted tuples while measuring memory on the next iteration.
        drop(tuple);
        let Some(reference) = BucketRef::acquire(bucket_id, sharded)? else {
            continue;
        };
        let deletion = space.delete(&(key.as_str(),));
        reference.release()?;
        match deletion {
            Ok(tuple) => batch.deleted += usize::from(tuple.is_some()),
            Err(error) => {
                if lua.eval::<bool>("return box.info.ro")? {
                    break;
                }
                return Err(error.into());
            }
        }
    }
    Ok(batch)
}
