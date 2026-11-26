use super::storage::{Coordinates, CACHE_GET_ROUTE, CACHE_PUT_ROUTE};
use serde_bytes::{ByteBuf, Bytes};
use serde_json::{Map, Value};
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

type Forecast = Map<String, Value>;

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
                        "Expected latitude and longitude lists with the same number of coordinates",
                    ));
                };
                Ok(fetch_forecasts(&coordinates))
            },
        );
    Server::new().register(Box::new(route));
    Ok(true)
}

fn parse_coordinates(query: &str) -> Option<Vec<Coordinates>> {
    let mut latitudes = None;
    let mut longitudes = None;
    for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
        let list = match name.as_ref() {
            "latitude" => &mut latitudes,
            "longitude" => &mut longitudes,
            _ => return None,
        };
        *list = Some(value);
    }
    let mut latitudes = latitudes.as_deref()?.split(',');
    let mut longitudes = longitudes.as_deref()?.split(',');
    let mut coordinates = Vec::new();
    loop {
        let (latitude, longitude) = match (latitudes.next(), longitudes.next()) {
            (Some(latitude), Some(longitude)) => (latitude, longitude),
            (None, None) => return Some(coordinates),
            _ => return None,
        };
        let latitude = latitude.parse::<f64>().ok()?;
        let longitude = longitude.parse::<f64>().ok()?;
        if !latitude.is_finite() || !longitude.is_finite() {
            return None;
        }
        coordinates.push(Coordinates {
            latitude,
            longitude,
        });
    }
}

fn fetch_forecasts(coordinates: &[Coordinates]) -> Response {
    let bucket_ids: Vec<_> = coordinates
        .iter()
        .map(|point| location_bucket_id(&format!("{},{}", point.latitude, point.longitude)))
        .collect();
    if let Some(body) = read_cached_forecasts(coordinates, &bucket_ids) {
        return forecast_response(200, body);
    }

    let (status, body) = match fetch_upstream_forecast(coordinates) {
        Ok(response) => response,
        Err(response) => return response,
    };

    if (200..300).contains(&status) {
        let Some(forecasts) = parse_forecasts(&body) else {
            return forecast_response(200, body);
        };
        cache_forecasts(coordinates, &bucket_ids, forecasts);
    }
    let response_status = if (400..500).contains(&status) {
        u32::from(status)
    } else {
        200
    };
    forecast_response(response_status, body)
}

fn parse_forecasts(body: &[u8]) -> Option<Vec<Forecast>> {
    match serde_json::from_slice::<Value>(body).ok()? {
        Value::Array(values) => values
            .into_iter()
            .map(|value| match value {
                Value::Object(forecast) => Some(forecast),
                _ => None,
            })
            .collect(),
        Value::Object(forecast) => Some(vec![forecast]),
        _ => None,
    }
}

fn fetch_upstream_forecast(coordinates: &[Coordinates]) -> Result<(u16, Vec<u8>), Response> {
    let (mut url, timeout) = FORECAST_CONFIG.with(|config| {
        let config = config.borrow();
        (config.upstream_url.clone(), config.upstream_timeout)
    });
    let latitudes = coordinates
        .iter()
        .map(|point| point.latitude.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let longitudes = coordinates
        .iter()
        .map(|point| point.longitude.to_string())
        .collect::<Vec<_>>()
        .join(",");
    url.query_pairs_mut()
        .append_pair("latitude", &latitudes)
        .append_pair("longitude", &longitudes);

    let client = fibreq::ClientBuilder::new().build();
    let result = client.get(url.as_str()).and_then(|request| {
        let mut upstream = request
            .tls_timeout(timeout)
            .request_timeout(timeout)
            .response_timeout(timeout)
            .send()?;
        let status = upstream.status();
        if status.is_server_error() {
            return Ok(None);
        }
        Ok(Some((u16::from(status), upstream.bytes()?)))
    });

    match result {
        Ok(Some(response)) => Ok(response),
        Err(err) if matches!(*err, fibreq::Error::Timeout) => {
            Err(error_response(504, "Open-Meteo request timed out"))
        }
        _ => Err(error_response(502, "Open-Meteo request failed")),
    }
}

fn read_cached_forecasts(
    coordinates: &[Coordinates],
    bucket_ids: &[Option<i64>],
) -> Option<Vec<u8>> {
    if let ([point], [bucket_id]) = (coordinates, bucket_ids) {
        return Some(read_cache((*bucket_id)?, *point)?.into_vec());
    }

    let mut response = vec![b'['];
    for (position, (&point, &bucket_id)) in coordinates.iter().zip(bucket_ids).enumerate() {
        let body = read_cache(bucket_id?, point)?;
        // Cache writes store compact JSON objects without location_id.
        if position == 0 {
            // The first forecast omits location_id, matching Open-Meteo's JSON format.
            response.extend_from_slice(&body);
            continue;
        }

        response.extend_from_slice(b",{\"location_id\":");
        response.extend_from_slice(position.to_string().as_bytes());
        response.push(b',');
        response.extend_from_slice(body.strip_prefix(b"{")?);
    }
    response.push(b']');
    Some(response)
}

fn cache_forecasts(
    coordinates: &[Coordinates],
    bucket_ids: &[Option<i64>],
    forecasts: Vec<Forecast>,
) {
    for ((&point, &bucket_id), mut forecast) in coordinates.iter().zip(bucket_ids).zip(forecasts) {
        if let Some(bucket_id) = bucket_id {
            // The location ID belongs to this batch, not the cached coordinate pair.
            forecast.remove("location_id");
            if let Ok(body) = serde_json::to_vec(&forecast) {
                write_cache(bucket_id, point, &body);
            }
        }
    }
}

fn location_bucket_id(key: &str) -> Option<i64> {
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
