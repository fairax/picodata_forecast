use shors::transport::http::{route::Builder, server::Server, Request, Response};
use shors::transport::Context;
use std::cell::RefCell;
use std::error::Error;
use std::time::Duration;
use tarantool::error::{BoxError, TarantoolErrorCode};
use tarantool::tuple::Tuple;
use url::Url;

const OPEN_METEO_FORECAST_URL: &str = "https://api.open-meteo.com/v1/forecast";
const OPEN_METEO_TIMEOUT: Duration = Duration::from_secs(10);

struct ForecastConfig {
    upstream_url: Url,
    upstream_timeout: Duration,
}

thread_local! {
    // Holding a config borrow across a yield can cause overlapping conflicting
    // borrows if another fiber applies configuration.
    static FORECAST_CONFIG: RefCell<ForecastConfig> = RefCell::new(ForecastConfig {
        upstream_url: Url::parse(OPEN_METEO_FORECAST_URL).expect("valid default Open-Meteo URL"),
        upstream_timeout: OPEN_METEO_TIMEOUT,
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
    let route = Builder::new()
        .with_method("GET")
        .with_path("/forecast")
        .build(
            |_: &mut Context, request: Request| -> Result<Response, Box<dyn Error>> {
                Ok(fetch_forecast(&request.query))
            },
        );
    Server::new().register(Box::new(route));
    Ok(true)
}

fn fetch_forecast(query: &str) -> Response {
    let (mut url, timeout) = FORECAST_CONFIG.with(|config| {
        let config = config.borrow();
        (config.upstream_url.clone(), config.upstream_timeout)
    });
    url.set_query((!query.is_empty()).then_some(query));
    let client = fibreq::ClientBuilder::new().build();
    let result = client.get(url.as_str()).and_then(|request| {
        request
            .tls_timeout(timeout)
            .request_timeout(timeout)
            .response_timeout(timeout)
            .send()?
            .bytes()
    });

    match result {
        Ok(body) => Response {
            status: 200,
            headers: Default::default(),
            body,
        },
        Err(err) if matches!(*err, fibreq::Error::Timeout) => {
            gateway_error(504, "Open-Meteo request timed out")
        }
        _ => gateway_error(502, "Open-Meteo request failed"),
    }
}

fn gateway_error(status: u32, message: &str) -> Response {
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
        Url::parse(upstream_url.unwrap_or(OPEN_METEO_FORECAST_URL))
            .ok()
            .filter(|url| matches!(url.scheme(), "http" | "https") && url.has_host())
            .ok_or_else(|| {
                BoxError::new(
                    TarantoolErrorCode::IllegalParams,
                    "forecast_router.upstream_url must be an HTTP(S) URL with a host",
                )
                .into()
            })
    }

    fn parse_upstream_timeout(timeout_seconds: Option<u64>) -> tarantool::Result<Duration> {
        match timeout_seconds {
            None => Ok(OPEN_METEO_TIMEOUT),
            Some(seconds) if seconds > 0 => Ok(Duration::from_secs(seconds)),
            Some(_) => Err(BoxError::new(
                TarantoolErrorCode::IllegalParams,
                "forecast_router.upstream_timeout_seconds must be a positive integer",
            )
            .into()),
        }
    }
}
