use super::storage::{Coordinates, CACHE_GET_ROUTE, CACHE_PUT_ROUTE};
use serde_bytes::{ByteBuf, Bytes};
use shors::transport::http::{route::Builder, server::Server, Request, Response};
use shors::transport::rpc::client;
use shors::transport::Context;
use std::cell::RefCell;
use std::error::Error;
use std::time::Duration;
use tarantool::error::{BoxError, TarantoolErrorCode};
use tarantool::tlua::{AsLua, StaticLua};
use tarantool::tuple::Tuple;
use url::Url;

const OPEN_METEO_FORECAST_URL_DEFAULT: &str = "https://api.open-meteo.com/v1/forecast";
const OPEN_METEO_TIMEOUT_DEFAULT: Duration = Duration::from_secs(10);

struct ForecastConfig {
    upstream_url: Url,
    upstream_timeout: Duration,
}

thread_local! {
    // Holding a config borrow across a yield can cause overlapping conflicting
    // borrows if another fiber applies configuration.
    static FORECAST_CONFIG: RefCell<ForecastConfig> = RefCell::new(ForecastConfig {
        upstream_url: Url::parse(OPEN_METEO_FORECAST_URL_DEFAULT).expect("valid default Open-Meteo URL"),
        upstream_timeout: OPEN_METEO_TIMEOUT_DEFAULT,
    });
}

#[tarantool::proc]
fn router_validate_config(settings: Option<ForecastSettings>) -> tarantool::Result<Tuple> {
    settings.unwrap_or_default().resolve()?;
    // Work around the pre-box.cfg() return-port crash by returning a Tuple.
    Tuple::new(&(true,))
}

#[tarantool::proc]
fn router_apply_config(settings: Option<ForecastSettings>) -> tarantool::Result<bool> {
    let config = settings.unwrap_or_default().resolve()?;
    FORECAST_CONFIG.with(|current| current.replace(config));
    tarantool::say_info!("Forecast router configuration loaded");
    Ok(true)
}

#[tarantool::proc]
pub fn router_init() -> tarantool::Result<bool> {
    let lua = tarantool::lua_state();
    let lua_view = unsafe { StaticLua::from_static(lua.as_lua()) };
    shors::init_lua_functions(&lua_view)?;

    let route = Builder::new()
        .with_method("GET")
        .with_path("/forecast")
        .build(
            |_: &mut Context, request: Request| -> Result<Response, Box<dyn Error>> {
                let Some(coordinates) = parse_coordinates(&request.query) else {
                    return Ok(error_response(
                        400,
                        "Expected exactly one latitude and one longitude",
                    ));
                };
                Ok(fetch_forecast(coordinates))
            },
        );
    Server::new().register(Box::new(route));
    Ok(true)
}

fn parse_coordinates(query: &str) -> Option<Coordinates> {
    let mut latitude = None;
    let mut longitude = None;
    for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
        let coordinate = match name.as_ref() {
            "latitude" => &mut latitude,
            "longitude" => &mut longitude,
            _ => return None,
        };
        *coordinate = Some(value.parse::<f64>().ok()?);
    }
    Some(Coordinates {
        latitude: latitude?,
        longitude: longitude?,
    })
}

fn fetch_forecast(coordinates: Coordinates) -> Response {
    let latitude = coordinates.latitude.to_string();
    let longitude = coordinates.longitude.to_string();
    let bucket_id = cache_bucket_id(&format!("{latitude},{longitude}"));
    if let Some(body) = bucket_id.and_then(|bucket_id| read_cache(bucket_id, coordinates)) {
        return forecast_response(200, body.into_vec());
    }

    let (mut url, timeout) = FORECAST_CONFIG.with(|config| {
        let config = config.borrow();
        (config.upstream_url.clone(), config.upstream_timeout)
    });
    url.query_pairs_mut()
        .append_pair("latitude", &latitude)
        .append_pair("longitude", &longitude);

    let client = fibreq::ClientBuilder::new().build();
    let result = client.get(url.as_str()).and_then(|request| {
        let mut upstream = request
            .tls_timeout(timeout)
            .request_timeout(timeout)
            .response_timeout(timeout)
            .send()?;
        let status = upstream.status();
        if status.is_server_error() {
            return Ok(error_response(502, "Open-Meteo request failed"));
        }

        let body = upstream.bytes()?;
        if status.is_success() {
            if let Some(bucket_id) = bucket_id {
                write_cache(bucket_id, coordinates, &body);
            }
        }
        let response_status = if status.is_client_error() {
            u32::from(u16::from(status))
        } else {
            200
        };
        Ok(forecast_response(response_status, body))
    });

    match result {
        Ok(response) => response,
        Err(err) if matches!(*err, fibreq::Error::Timeout) => {
            error_response(504, "Open-Meteo request timed out")
        }
        _ => error_response(502, "Open-Meteo request failed"),
    }
}

fn cache_bucket_id(key: &str) -> Option<i64> {
    tarantool::lua_state()
        .eval_with(
            "return require('vshard').router.bucket_id_strcrc32(...)",
            key,
        )
        .ok()
}

fn read_cache(bucket_id: i64, coordinates: Coordinates) -> Option<ByteBuf> {
    let lua = tarantool::lua_state();
    let response = client::Builder::new(&lua)
        .shard_endpoint(CACHE_GET_ROUTE)
        .call(&mut Context::background(), bucket_id, coordinates)
        .ok()?;
    response.decode::<(Option<ByteBuf>,)>().ok()?.0
}

fn write_cache(bucket_id: i64, coordinates: Coordinates, body: &[u8]) {
    let lua = tarantool::lua_state();
    let _ = client::Builder::new(&lua)
        .shard_endpoint(CACHE_PUT_ROUTE)
        .call(
            &mut Context::background(),
            bucket_id,
            (coordinates, bucket_id, Bytes::new(body)),
        );
}

fn forecast_response(status: u32, body: Vec<u8>) -> Response {
    Response {
        status,
        headers: Default::default(),
        body,
    }
}

fn error_response(status: u32, message: &str) -> Response {
    Response {
        status,
        headers: [("content-type".into(), "text/plain; charset=utf-8".into())].into(),
        body: message.as_bytes().to_vec(),
    }
}

#[derive(Default, serde::Deserialize)]
#[serde(default)]
struct ForecastSettings {
    upstream_url: Option<String>,
    upstream_timeout_seconds: Option<u64>,
}

impl ForecastSettings {
    fn resolve(self) -> tarantool::Result<ForecastConfig> {
        Ok(ForecastConfig {
            upstream_url: Self::parse_upstream_url(self.upstream_url.as_deref())?,
            upstream_timeout: Self::parse_upstream_timeout(self.upstream_timeout_seconds)?,
        })
    }

    fn parse_upstream_url(upstream_url: Option<&str>) -> tarantool::Result<Url> {
        Url::parse(upstream_url.unwrap_or(OPEN_METEO_FORECAST_URL_DEFAULT))
            .ok()
            .filter(|url| {
                matches!(url.scheme(), "http" | "https") && url.has_host() && url.query().is_none()
            })
            .ok_or_else(|| {
                BoxError::new(
                    TarantoolErrorCode::IllegalParams,
                    "forecast_router.upstream_url must be an HTTP(S) URL with a host and no query string",
                )
                .into()
            })
    }

    fn parse_upstream_timeout(timeout_seconds: Option<u64>) -> tarantool::Result<Duration> {
        match timeout_seconds {
            None => Ok(OPEN_METEO_TIMEOUT_DEFAULT),
            Some(seconds) if seconds > 0 => Ok(Duration::from_secs(seconds)),
            Some(_) => Err(BoxError::new(
                TarantoolErrorCode::IllegalParams,
                "forecast_router.upstream_timeout_seconds must be a positive integer",
            )
            .into()),
        }
    }
}
