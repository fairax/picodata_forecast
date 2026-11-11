use std::error::Error as _;
use std::io::ErrorKind;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime};
use std::{collections::BTreeMap, ops::Deref};

use serde::{Deserialize, Serialize};
use tarantool::tuple::Tuple;

use crate::{Result, storage::CacheEntry};

#[derive(Debug)]
pub enum FetchError {
    Timeout(String),
    Transport(String),
    Internal(String),
}

impl FetchError {
    pub fn internal(error: impl std::fmt::Display) -> Self {
        Self::Internal(error.to_string())
    }

    fn http(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            Self::Timeout(error.to_string())
        } else if error.is_builder() {
            Self::Internal(error.to_string())
        } else {
            Self::Transport(error.to_string())
        }
    }
}

impl From<FetchError> for Response {
    fn from(error: FetchError) -> Self {
        let (status, body, detail) = match error {
            FetchError::Timeout(detail) => (504, "Forecast request timed out", detail),
            FetchError::Transport(detail) => (502, "Forecast service unavailable", detail),
            FetchError::Internal(detail) => (500, "Internal server error", detail),
        };
        // Called only on the Tarantool thread, after the HTTP worker returns.
        tarantool::say_error!("Forecast request failed: {}", detail);
        Response {
            status,
            body: body.to_owned(),
            headers: BTreeMap::from([("content-type".into(), "text/plain; charset=utf-8".into())]),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Response {
    pub status: u16,
    pub body: String,
    pub headers: BTreeMap<String, String>,
}

impl TryFrom<&Tuple> for Response {
    type Error = Box<dyn std::error::Error>;

    fn try_from(tuple: &Tuple) -> Result<Self> {
        let entry = CacheEntry(tuple);
        Ok(Self {
            status: entry.status()?,
            body: entry.body()?,
            headers: entry.headers()?,
        })
    }
}

impl TryFrom<Tuple> for Response {
    type Error = Box<dyn std::error::Error>;

    fn try_from(tuple: Tuple) -> Result<Self> {
        Self::try_from(&tuple)
    }
}

pub struct NormalizedQuery(String);

impl NormalizedQuery {
    pub fn new(query: &str) -> NormalizedQuery {
        let mut params: Vec<_> = form_urlencoded::parse(query.as_bytes()).collect();
        params.sort_by(|a, b| a.0.cmp(&b.0));

        let query = form_urlencoded::Serializer::new(String::new())
            .extend_pairs(params)
            .finish();
        NormalizedQuery(query)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Deref for NormalizedQuery {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

struct ForecastConfig {
    endpoint: reqwest::Url,
    timeout: Duration,
}

static CONFIG: OnceLock<ForecastConfig> = OnceLock::new();

pub fn initialize() -> std::result::Result<(), FetchError> {
    if CONFIG.get().is_some() {
        return Ok(());
    }
    let endpoint = match std::env::var("OPEN_METEO_FORECAST_URL") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "https://api.open-meteo.com/v1/forecast".to_owned(),
        Err(error) => return Err(FetchError::internal(error)),
    };
    let endpoint = reqwest::Url::parse(&endpoint).map_err(FetchError::internal)?;
    if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() {
        return Err(FetchError::internal(
            "OPEN_METEO_FORECAST_URL must be an HTTP or HTTPS URL with a host",
        ));
    }
    let timeout = fetch_timeout()?;
    Instant::now().checked_add(timeout).ok_or_else(|| {
        FetchError::internal("OPEN_METEO_TIMEOUT_SECS exceeds the supported deadline range")
    })?;
    CONFIG
        .set(ForecastConfig { endpoint, timeout })
        .map_err(|_| FetchError::internal("Forecast configuration is already initialized"))
}

/// Fetch a forecast using configuration saved by [initialize].
///
/// Run only in the `coio_call` worker: HTTP and backoff block the calling thread.
/// Retry transient transport failures and HTTP 429/502/503/504 up to three attempts,
/// reusing one client with built-in retries disabled to avoid multiplying attempts.
/// Attempts are capped at 10 seconds and share `OPEN_METEO_TIMEOUT_SECS` (default 30)
/// with retry delays; worker queue time is excluded. A monotonic deadline prevents
/// clock changes from extending the budget.
///
/// Jittered backoff (0–200 ms, then 0–400 ms) spreads retry load. Honor `Retry-After`
/// seconds or dates; return the last result if the delay would exhaust the budget.
/// Permanent failures are not retried. Redirects follow Reqwest's default policy.
///
/// HTTP errors remain `Ok(Response)` for forwarding; transport/internal failures
/// return [FetchError]. The caller logs/maps errors and caches only 2xx responses.
/// This function performs no cache access, request coalescing, or Tarantool calls.
pub fn fetch(query: &NormalizedQuery) -> std::result::Result<Response, FetchError> {
    let config = CONFIG
        .get()
        .ok_or_else(|| FetchError::internal("Forecast configuration has not been initialized"))?;
    let mut url = config.endpoint.clone();
    url.set_query(Some(query.as_str()));
    
    // Share one deadline across attempts, backoff, and Retry-After delays so
    // retries cannot each restart the full timeout budget.
    let deadline = Instant::now().checked_add(config.timeout).ok_or_else(|| {
        FetchError::internal("OPEN_METEO_TIMEOUT_SECS exceeds the supported deadline range")
    })?;
    let client = reqwest::blocking::Client::builder()
        .retry(reqwest::retry::never())
        .build()
        .map_err(FetchError::internal)?;

    for attempt in 0..3 {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(FetchError::Timeout(
                "Forecast retry deadline exceeded".into(),
            ));
        }
        let timeout = remaining.min(Duration::from_secs(10));
        let outcome = fetch_once(&client, &url, timeout);
        let delay = Duration::from_millis(fastrand::u64(0..=200 * (1 << attempt)));
        let (failure, delay) = match outcome {
            Ok(response) => {
                if attempt == 2 || !matches!(response.status, 429 | 502 | 503 | 504) {
                    return Ok(response);
                }
                let retry_after = response
                    .headers
                    .get("retry-after")
                    .and_then(|value| retry_after(value, SystemTime::now()))
                    .unwrap_or_default();
                (Ok(response), delay.max(retry_after))
            }
            Err(error) => {
                let retry = attempt < 2 && retryable_transport(&error);
                let error = FetchError::http(error);
                if !retry {
                    return Err(error);
                }
                (Err(error), delay)
            }
        };
        // Preserve the last response/error if there is no time for another attempt.
        if delay >= deadline.saturating_duration_since(Instant::now()) {
            return failure;
        }
        drop(failure); // Release body buffers and failed attempt resources before waiting.
        std::thread::sleep(delay); // fetch() runs only in the HTTP worker.
    }
    unreachable!("Every final attempt returns its response or error")
}

fn fetch_timeout() -> std::result::Result<Duration, FetchError> {
    let value = match std::env::var("OPEN_METEO_TIMEOUT_SECS") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(Duration::from_secs(30)),
        Err(error) => {
            return Err(FetchError::internal(format!(
                "OPEN_METEO_TIMEOUT_SECS: {error}"
            )));
        }
    };
    let seconds = value
        .parse::<u64>()
        .ok()
        .filter(|&seconds| seconds > 0)
        .ok_or_else(|| {
            FetchError::internal(
                "OPEN_METEO_TIMEOUT_SECS must be a positive integer number of seconds",
            )
        })?;
    Ok(Duration::from_secs(seconds))
}

fn retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        // An overflowing numeric delay is still over budget, not a malformed date.
        return Some(Duration::from_secs(value.parse().unwrap_or(u64::MAX)));
    }
    httpdate::parse_http_date(value)
        .ok()
        .map(|date| date.duration_since(now).unwrap_or_default())
}

fn retryable_transport(error: &reqwest::Error) -> bool {
    if error.is_timeout() {
        return true;
    }
    let mut source = error.source();
    while let Some(cause) = source {
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            return matches!(
                io.kind(),
                ErrorKind::ConnectionRefused
                    | ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::BrokenPipe
                    | ErrorKind::TimedOut
                    | ErrorKind::UnexpectedEof
                    | ErrorKind::WouldBlock
                    | ErrorKind::Interrupted
                    | ErrorKind::NotConnected
            );
        }
        source = cause.source();
    }
    false
}

fn fetch_once(
    client: &reqwest::blocking::Client,
    url: &reqwest::Url,
    timeout: Duration,
) -> reqwest::Result<Response> {
    let response = client.get(url.clone()).timeout(timeout).send()?;
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or_default().to_string()))
        .collect::<BTreeMap<_, _>>();
    let body = response.text()?;
    Ok(Response {
        status,
        body,
        headers,
    })
}
