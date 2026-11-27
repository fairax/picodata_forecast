use serde::de::IgnoredAny;
use serde_bytes::ByteBuf;
use shors::transport::rpc::server::Server;
use shors::transport::{rpc, Context};
use std::cell::{OnceCell, RefCell};
use std::ffi::c_int;
use tarantool::clock;
use tarantool::error::{BoxError, TarantoolErrorCode};
use tarantool::index::IndexOptions;
use tarantool::space::{Field, Space};
use tarantool::tuple::{FunctionArgs, FunctionCtx, Tuple};

const CACHE_SPACE: &str = "forecast_cache";
pub const CACHE_GET_ROUTE: &str = "/forecast/cache/get";
pub const CACHE_PUT_ROUTE: &str = "/forecast/cache/put";

const CACHE_TTL_SECONDS_DEFAULT: f64 = 60.0;
const MAX_CACHE_ENTRIES_PER_SHARD_DEFAULT: usize = 10_000;

#[derive(Clone, Copy)]
struct StorageConfig {
    cache_ttl_seconds: f64,
    max_cache_entries_per_shard: usize,
}

const STORAGE_CONFIG_DEFAULT: StorageConfig = StorageConfig {
    cache_ttl_seconds: CACHE_TTL_SECONDS_DEFAULT,
    max_cache_entries_per_shard: MAX_CACHE_ENTRIES_PER_SHARD_DEFAULT,
};

#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct Coordinates {
    pub latitude: f64,
    pub longitude: f64,
}

thread_local! {
    static CACHE_RPC_SERVER: OnceCell<Server> = const { OnceCell::new() };
    // Copy the settings before any operation that can yield so another fiber can apply config.
    static STORAGE_CONFIG: RefCell<StorageConfig> = const { RefCell::new(STORAGE_CONFIG_DEFAULT) };
}

#[tarantool::proc]
fn storage_validate_config(settings: Option<StorageSettings>) -> tarantool::Result<Tuple> {
    settings.unwrap_or_default().resolve()?;
    // Work around the pre-box.cfg() return-port crash by returning a Tuple.
    Tuple::new(&(true,))
}

#[tarantool::proc]
fn storage_apply_config(settings: Option<StorageSettings>) -> tarantool::Result<bool> {
    let config = settings.unwrap_or_default().resolve()?;
    STORAGE_CONFIG.with(|current| current.replace(config));
    tarantool::say_info!("Forecast storage configuration loaded");
    Ok(true)
}

fn init_cache_rpc() -> Server {
    let server = Server::new();
    let get = rpc::route::Builder::new().with_path(CACHE_GET_ROUTE).build(
        |_: &mut Context, request: rpc::Request| {
            request
                .parse()
                .and_then(get_cache_entry)
                .inspect_err(|error| tarantool::say_error!("Forecast cache read failed: {error}"))
        },
    );
    let put = rpc::route::Builder::new().with_path(CACHE_PUT_ROUTE).build(
        |_: &mut Context, request: rpc::Request| {
            request
                .parse()
                .and_then(|(coordinates, bucket_id, body)| {
                    put_cache_entry(coordinates, bucket_id, body)
                })
                .inspect_err(|error| tarantool::say_error!("Forecast cache write failed: {error}"))
        },
    );
    server.register(Box::new(get));
    server.register(Box::new(put));
    server
}

#[no_mangle]
pub extern "C" fn rpc_handler(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    CACHE_RPC_SERVER.with(|server| server.get_or_init(init_cache_rpc).handle(ctx, args))
}

#[tarantool::proc]
pub fn storage_init() -> tarantool::Result<bool> {
    tarantool::say_info!("Initializing forecast cache storage");
    create_schema()?;
    CACHE_RPC_SERVER.with(|server| {
        server.get_or_init(init_cache_rpc);
    });
    tarantool::say_info!("Forecast cache schema and RPC routes are ready");
    Ok(true)
}

fn create_schema() -> tarantool::Result<()> {
    let space = Space::builder(CACHE_SPACE)
        .if_not_exists(true)
        .format([
            Field::double("latitude"),
            Field::double("longitude"),
            Field::unsigned("bucket_id"),
            Field::varbinary("body"),
            Field::double("expires_at"),
            Field::double("last_access"),
        ])
        .create()?;

    space.create_index(
        "primary",
        &IndexOptions {
            if_not_exists: Some(true),
            parts: Some(vec!["latitude".into(), "longitude".into()]),
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
            parts: Some(vec!["expires_at".into()]),
            ..Default::default()
        },
    )?;

    space.create_index(
        "lru",
        &IndexOptions {
            unique: Some(false),
            if_not_exists: Some(true),
            parts: Some(vec!["last_access".into()]),
            ..Default::default()
        },
    )?;

    Ok(())
}

#[derive(serde::Deserialize)]
pub struct CacheRecord {
    pub latitude: f64,
    pub longitude: f64,
    pub _bucket_id: IgnoredAny,
    pub body: ByteBuf,
    pub expires_at: f64,
    pub _last_access: IgnoredAny,
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

fn get_cache_entry(coordinates: Coordinates) -> tarantool::Result<Option<ByteBuf>> {
    let now = clock::time();
    let space = cache_space()?;
    let key = (coordinates.latitude, coordinates.longitude);
    let Some(tuple) = space.get(&key)? else {
        return Ok(None);
    };
    let entry: CacheRecord = tuple.decode()?;
    if entry.expires_at <= now {
        space.delete(&key)?;
        return Ok(None);
    }
    space.update(&key, [("=", "last_access", now)])?;
    Ok(Some(entry.body))
}

fn put_cache_entry(
    coordinates: Coordinates,
    bucket_id: u64,
    body: ByteBuf,
) -> tarantool::Result<bool> {
    let config = STORAGE_CONFIG.with(|current| *current.borrow());
    let now = clock::time();
    let expires_at = now + config.cache_ttl_seconds;
    let space = cache_space()?;

    let count = space.len()?;
    let needs_eviction = count >= config.max_cache_entries_per_shard;

    if needs_eviction {
        // Reclaim an expired entry before evicting a forecast that can still be served.
        let candidate = match oldest_cache_entry(&space, "expires_at")? {
            Some(entry) if entry.expires_at <= now => Some(entry),
            _ => oldest_cache_entry(&space, "lru")?,
        };

        if let Some(candidate) = candidate {
            let candidate_key = (candidate.latitude, candidate.longitude);
            // The replacement itself refreshes the eviction candidate when the keys match.
            if candidate_key != (coordinates.latitude, coordinates.longitude) {
                space.delete(&candidate_key)?;
            }
        }
    }

    space.replace(&(
        coordinates.latitude,
        coordinates.longitude,
        bucket_id,
        body,
        expires_at,
        now,
    ))?;
    Ok(true)
}

fn oldest_cache_entry(space: &Space, index_name: &str) -> tarantool::Result<Option<CacheRecord>> {
    let index = space.index(index_name).ok_or_else(|| {
        BoxError::new(
            TarantoolErrorCode::NoSuchIndexName,
            format!("Cache index '{index_name}' does not exist"),
        )
    })?;
    index.min(&())?.map(|tuple| tuple.decode()).transpose()
}

#[derive(Default, serde::Deserialize)]
#[serde(default)]
struct StorageSettings {
    cache_ttl_seconds: Option<f64>,
    max_cache_entries_per_shard: Option<usize>,
}

impl StorageSettings {
    fn resolve(self) -> tarantool::Result<StorageConfig> {
        let cache_ttl_seconds = self.cache_ttl_seconds.unwrap_or(CACHE_TTL_SECONDS_DEFAULT);
        if !cache_ttl_seconds.is_finite() || cache_ttl_seconds <= 0.0 {
            return Err(BoxError::new(
                TarantoolErrorCode::IllegalParams,
                "forecast_storage.cache_ttl_seconds must be a finite positive number",
            )
            .into());
        }

        let max_cache_entries_per_shard = self
            .max_cache_entries_per_shard
            .unwrap_or(MAX_CACHE_ENTRIES_PER_SHARD_DEFAULT);
        if max_cache_entries_per_shard == 0 {
            return Err(BoxError::new(
                TarantoolErrorCode::IllegalParams,
                "forecast_storage.max_cache_entries_per_shard must be a positive integer",
            )
            .into());
        }

        Ok(StorageConfig {
            cache_ttl_seconds,
            max_cache_entries_per_shard,
        })
    }
}
