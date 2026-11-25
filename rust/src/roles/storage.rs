use serde_bytes::ByteBuf;
use shors::transport::rpc::server::Server;
use shors::transport::{rpc, Context};
use std::cell::OnceCell;
use std::ffi::c_int;
use tarantool::clock;
use tarantool::error::{BoxError, TarantoolErrorCode};
use tarantool::index::IndexOptions;
use tarantool::space::{Field, Space};
use tarantool::tuple::{FunctionArgs, FunctionCtx};

const CACHE_SPACE: &str = "forecast_cache";
pub const CACHE_GET_ROUTE: &str = "/forecast/cache/get";
pub const CACHE_PUT_ROUTE: &str = "/forecast/cache/put";

const CACHE_TTL_SECONDS_DEFAULT: f64 = 60.0;

thread_local! {
    static CACHE_RPC_SERVER: OnceCell<Server> = const { OnceCell::new() };
}

fn init_cache_rpc() -> Server {
    let server = Server::new();
    let get = rpc::route::Builder::new()
        .with_path(CACHE_GET_ROUTE)
        .build(|_: &mut Context, request: rpc::Request| get_cache_entry(request.parse()?));
    let put = rpc::route::Builder::new().with_path(CACHE_PUT_ROUTE).build(
        |_: &mut Context, request: rpc::Request| {
            let (key, bucket_id, body) = request.parse()?;
            put_cache_entry(key, bucket_id, body)
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
            Field::string("key"),
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
    pub expires_at: f64,
    pub last_access: f64,
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

fn get_cache_entry(key: String) -> tarantool::Result<Option<ByteBuf>> {
    let now = clock::time();
    let space = cache_space()?;
    let key = (key,);
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

fn put_cache_entry(key: String, bucket_id: u64, body: ByteBuf) -> tarantool::Result<bool> {
    let now = clock::time();
    let expires_at = now + CACHE_TTL_SECONDS_DEFAULT;
    cache_space()?.replace(&(key, bucket_id, body, expires_at, now))?;
    Ok(true)
}
