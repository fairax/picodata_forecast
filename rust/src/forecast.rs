use std::time::Duration;
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

pub fn fetch(query: &NormalizedQuery) -> std::result::Result<Response, FetchError> {
    let endpoint = match std::env::var("OPEN_METEO_FORECAST_URL") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "https://api.open-meteo.com/v1/forecast".to_owned(),
        Err(error) => return Err(FetchError::internal(error)),
    };
    let mut url = reqwest::Url::parse(&endpoint).map_err(FetchError::internal)?;
    url.set_query(Some(query.as_str()));
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(FetchError::internal)?;
    let response = client.get(url).send().map_err(FetchError::http)?;
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or_default().to_string()))
        .collect::<BTreeMap<_, _>>();
    let body = response.text().map_err(FetchError::http)?;
    Ok(Response {
        status,
        body,
        headers,
    })
}
