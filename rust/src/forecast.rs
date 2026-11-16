use std::io::ErrorKind;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime};
use std::{collections::BTreeMap, ops::Deref};

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
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
}

impl From<FetchError> for Response {
    fn from(error: FetchError) -> Self {
        let (status, body, detail) = match error {
            FetchError::Timeout(detail) => (504, "Forecast request timed out", detail),
            FetchError::Transport(detail) => (502, "Forecast service unavailable", detail),
            FetchError::Internal(detail) => (500, "Internal server error", detail),
        };
        // Fetching and error reporting both run on the Tarantool thread.
        tarantool::say_error!("Forecast request failed: {}", detail);
        Response {
            status,
            body: ByteBuf::from(body),
            headers: BTreeMap::from([("content-type".into(), "text/plain; charset=utf-8".into())]),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Response {
    pub status: u16,
    pub body: ByteBuf,
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
    endpoint: url::Url,
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
    let endpoint = url::Url::parse(&endpoint).map_err(FetchError::internal)?;
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

/// Fetch on the calling Tarantool fiber, yielding during network I/O and backoff.
/// Retry temporary failures up to three times, with jitter and Retry-After.
/// The deadline gates attempts; fibreq uses separate phase timeouts,
/// so one exchange can outlive the remaining overall budget. Responses are fully
/// buffered without a size limit because fibreq 0.2 has no bounded body-reading API.
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

    for attempt in 0..3 {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(FetchError::Timeout(
                "Forecast retry deadline exceeded".into(),
            ));
        }
        let outcome = fetch_once(url.clone(), deadline);
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
                let failure = error.classify();
                if attempt == 2 || !failure.retryable {
                    return Err(failure.error);
                }
                (Err(failure.error), delay)
            }
        };
        // Preserve the last response/error if there is no time for another attempt.
        if delay >= deadline.saturating_duration_since(Instant::now()) {
            return failure;
        }
        drop(failure); // Release body buffers and failed attempt resources before waiting.
        tarantool::fiber::sleep(delay);
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

fn temporary_cause(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(cause) = current {
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            if retryable_io(io) {
                return true;
            }
        }
        current = cause.source();
    }
    false
}

fn retryable_io(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::BrokenPipe
            | ErrorKind::TimedOut
            | ErrorKind::UnexpectedEof
            | ErrorKind::WouldBlock
            | ErrorKind::Interrupted
            | ErrorKind::NotConnected
    )
}

enum AttemptError {
    Http(Box<fibreq::Error>),
    IncompleteBody { expected: u64, received: usize },
    Deadline,
}

struct ClassifiedFailure {
    error: FetchError,
    retryable: bool,
}

impl AttemptError {
    fn classify(self) -> ClassifiedFailure {
        match self {
            Self::Http(error) => {
                use fibreq::Error;
                let timeout = matches!(
                    *error,
                    Error::Timeout | Error::TCP(tarantool::network::client::tcp::Error::Timeout)
                );
                let temporary = match error.as_ref() {
                    Error::HTTP(cause) => temporary_cause(cause.as_ref()),
                    Error::IO(cause) => retryable_io(cause),
                    Error::TCP(tarantool::network::client::tcp::Error::Connect {
                        error, ..
                    }) => retryable_io(error),
                    _ => temporary_cause(error.as_ref()),
                };
                let retryable = timeout || matches!(*error, Error::SocketClosed) || temporary;
                let detail = error.to_string();
                ClassifiedFailure {
                    error: if timeout {
                        FetchError::Timeout(detail)
                    } else {
                        FetchError::Transport(detail)
                    },
                    retryable: retryable && !matches!(*error, Error::TLS(_)),
                }
            }
            Self::IncompleteBody { expected, received } => ClassifiedFailure {
                error: FetchError::Transport(format!(
                    "Incomplete upstream body: expected {expected} bytes, received {received}"
                )),
                retryable: true,
            },
            Self::Deadline => ClassifiedFailure {
                error: FetchError::Timeout("Forecast retry deadline exceeded".into()),
                retryable: false,
            },
        }
    }
}

fn fetch_once(url: url::Url, deadline: Instant) -> std::result::Result<Response, AttemptError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(AttemptError::Deadline);
    }
    let timeout = remaining.min(Duration::from_secs(10));
    // A private client per exchange avoids reusing a socket before its body
    // is consumed, and avoids repeated TLS handshakes on a pooled TLS socket.
    let client = fibreq::ClientBuilder::new()
        .pool_max_conns(1)
        .pool_connect_timeout(timeout)
        .pool_acquire_timeout(timeout)
        .build();
    let mut response = client
        .get(url.as_str())
        .map_err(AttemptError::Http)?
        .tls_timeout(timeout)
        .request_timeout(timeout)
        .response_timeout(timeout)
        .send()
        .map_err(AttemptError::Http)?;
    let status = response.status() as u16;
    let headers = response
        .headers()
        .into_iter()
        // fibreq's HTTP decoder synthesizes a Date value containing CRLF.
        // Never forward values that could inject HTTP header boundaries.
        .filter(|(_, values)| !values.last().as_str().contains(['\r', '\n']))
        .map(|(name, values)| (name.as_str().to_owned(), values.last().as_str().to_owned()))
        .collect::<BTreeMap<_, _>>();
    let bytes = if (100..200).contains(&status) || matches!(status, 204 | 304) {
        // Content-Length on 304 describes the representation, not a response body.
        Vec::new()
    } else {
        let expected = response.content_length();
        let bytes = response.bytes().map_err(AttemptError::Http)?;
        if let Some(expected) = expected {
            if bytes.len() as u64 != expected {
                return Err(AttemptError::IncompleteBody {
                    expected,
                    received: bytes.len(),
                });
            }
        }
        bytes
    };
    Ok(Response {
        status,
        body: ByteBuf::from(bytes),
        headers,
    })
}
