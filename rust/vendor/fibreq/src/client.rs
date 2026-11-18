//! The `client` module provides the `Client` struct for making HTTP requests.
//!
//! This module includes the main HTTP client capabilities for performing
//! various HTTP methods such as GET, POST, PUT, PATCH, DELETE, and HEAD.
//! It utilizes a connection pool for efficient management of connections.

use crate::{connection, error, request, response};
use std::str::FromStr;
use std::{rc, time};

/// Represents an HTTP client that manages connections and executes HTTP requests.
///
/// The client is designed to be efficient and reusable, utilizing a connection pool
/// under the hood. It supports making HTTP requests with different methods and automatically
/// handles connection pooling and reuse.
#[derive(Clone, Debug)]
pub struct Client {
    pool: rc::Rc<connection::Pool>,
    proxy: Option<crate::Proxy>,
    proxy_connect_timeout: time::Duration,
}

impl Client {
    /// Constructs a new `Client` instance with a specified connection pool.
    ///
    /// This constructor is primarily used internally within the library.
    ///
    /// # Parameters
    ///
    /// - `pool`: The connection pool to be used by the client.
    pub(crate) fn new(pool: rc::Rc<connection::Pool>) -> Self {
        Self { pool, proxy: None, proxy_connect_timeout: time::Duration::from_secs(3) }
    }

    /// Initiates an HTTP GET request.
    ///
    /// # Parameters
    ///
    /// - `url`: The URL to request.
    ///
    /// # Returns
    ///
    /// A `Result` wrapping a `request::Builder` if successful, or an `error::Error` in case of failure.
    ///
    /// # Errors
    ///
    /// Returns `error::Error::URL` in case failed url parse.
    pub fn get(&self, url: impl AsRef<str>) -> Result<request::Builder, Box<error::Error>> {
        self.request(http_types::Method::Get, url)
    }

    /// Initiates an HTTP POST request.
    ///
    /// # Parameters
    ///
    /// - `url`: The URL to request.
    ///
    /// # Returns
    ///
    /// A `Result` wrapping a `request::Builder` if successful, or an `error::Error` in case of failure.
    ///
    /// # Errors
    ///
    /// Returns `error::Error::URL` in case failed url parse.
    pub fn post(&self, url: impl AsRef<str>) -> Result<request::Builder, Box<error::Error>> {
        self.request(http_types::Method::Post, url)
    }

    /// Initiates an HTTP PUT request.
    ///
    /// # Parameters
    ///
    /// - `url`: The URL to request.
    ///
    /// # Returns
    ///
    /// A `Result` wrapping a `request::Builder` if successful, or an `error::Error` in case of failure.
    ///
    /// # Errors
    ///
    /// Returns `error::Error::URL` in case failed url parse.
    pub fn put(&self, url: impl AsRef<str>) -> Result<request::Builder, Box<error::Error>> {
        self.request(http_types::Method::Put, url)
    }

    /// Initiates an HTTP PATCH request.
    ///
    /// # Parameters
    ///
    /// - `url`: The URL to request.
    ///
    /// # Returns
    ///
    /// A `Result` wrapping a `request::Builder` if successful, or an `error::Error` in case of failure.
    ///
    /// # Errors
    ///
    /// Returns `error::Error::URL` in case failed url parse.
    pub fn patch(&self, url: impl AsRef<str>) -> Result<request::Builder, Box<error::Error>> {
        self.request(http_types::Method::Patch, url)
    }

    /// Initiates an HTTP DELETE request.
    ///
    /// # Parameters
    ///
    /// - `url`: The URL to request.
    ///
    /// # Returns
    ///
    /// A `Result` wrapping a `request::Builder` if successful, or an `error::Error` in case of failure.
    ///
    /// # Errors
    ///
    /// Returns `error::Error::URL` in case failed url parse.
    pub fn delete(&self, url: impl AsRef<str>) -> Result<request::Builder, Box<error::Error>> {
        self.request(http_types::Method::Delete, url)
    }

    /// Initiates an HTTP HEAD request.
    ///
    /// # Parameters
    ///
    /// - `url`: The URL to request.
    ///
    /// # Returns
    ///
    /// A `Result` wrapping a `request::Builder` if successful, or an `error::Error` in case of failure.
    ///
    /// # Errors
    ///
    /// Returns `error::Error::URL` in case failed url parse.
    pub fn head(&self, url: impl AsRef<str>) -> Result<request::Builder, Box<error::Error>> {
        self.request(http_types::Method::Head, url)
    }

    /// Initiates an HTTP request with provided method.
    ///
    /// # Parameters
    ///
    /// - `url`: The URL to request.
    ///
    /// # Returns
    ///
    /// A `Result` wrapping a `request::Builder` if successful, or an `error::Error` in case of failure.
    ///
    /// # Errors
    ///
    /// Returns `error::Error::URL` in case failed url parse.
    pub fn request(
        &self,
        method: impl AsRef<str>,
        url: impl AsRef<str>,
    ) -> Result<request::Builder, Box<error::Error>> {
        let url =
            http_types::Url::parse(url.as_ref()).map_err(|e| Box::new(error::Error::URL(e)))?;
        let method =
            http_types::Method::from_str(method.as_ref()).map_err(Box::new(error::Error::HTTP))?;
        Ok(request::Builder::new(
            request::Request::new(method, url),
            self.clone(),
        ))
    }

    /// Executes an HTTP request using the client's connection pool.
    ///
    /// # Parameters
    ///
    /// - `request`: The `request::Request` to be executed.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `response::Response` if the request is successful,
    /// or an `error::Error` if an error occurs during the request execution.
    ///
    /// # Errors
    ///
    ///-  Returns `error::Error::EmptyHost` in case of empty url host.
    /// - Returns `error::Error::UnsupportedSchema` in case of invalid schema and missing port.
    /// - Returns `error::Error::TCP` in case of failed tcp connect.
    /// - Returns `error::Error::TLS` in case of failed tls connect.
    /// - Returns `error::Error::HTTP` in case of failed request sending.
    /// - Returns `error::Error::Timeout` in case of any operation timeout.
    pub fn execute(
        &self,
        request: request::Request,
    ) -> Result<response::Response, Box<error::Error>> {
        if let Some(proxy) = &self.proxy {
            return crate::proxy::execute(proxy, request, self.proxy_connect_timeout);
        }
        let host = request.url().host_str().ok_or(error::Error::EmptyHost)?;
        let port = if let Some(v) = request.url().port() {
            v
        } else {
            match request.url().scheme() {
                "http" => 80,
                "https" => 443,
                _ => return Err(Box::new(error::Error::UnsupportedSchema)),
            }
        };

        let mut connection = self.pool.get(host, port)?;

        connection.execute(request)
    }
}

/// A builder for creating instances of `Client` with custom configurations.
///
/// Allows customization of the connection pool parameters such as maximum connections,
/// connection time-to-live (TTL), connection timeout, and acquisition timeout.
#[derive(Clone, Debug)]
pub struct Builder {
    proxy: Option<crate::Proxy>,
    max_conns: Option<usize>,
    conn_ttl: Option<time::Duration>,
    connect_timeout: Option<time::Duration>,
    acquire_timeout: Option<time::Duration>,
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

impl Builder {
    /// Constructs a new `Builder` instance with default settings.
    pub fn new() -> Self {
        Self {
            proxy: None,
            max_conns: None,
            conn_ttl: None,
            connect_timeout: None,
            acquire_timeout: None,
        }
    }

    /// Sets the maximum number of connections for the pool.
    ///
    /// # Parameters
    ///
    /// - `max_conns`: The maximum number of connections.
    ///
    /// # Returns
    ///
    /// Returns the `Builder` instance for chaining.
    pub fn pool_max_conns(mut self, max_conns: usize) -> Self {
        self.max_conns = Some(max_conns);
        self
    }

    /// Use an HTTP proxy for every request. Unset means direct access; ambient
    /// proxy environment variables are not consulted. Proxied sockets are not pooled.
    pub fn proxy(mut self, proxy: crate::Proxy) -> Self {
        self.proxy = Some(proxy);
        self
    }

    /// Sets the ttl of connection for the pool.
    ///
    /// # Parameters
    ///
    /// - `ttl`: The duration of ttl for connection.
    ///
    /// # Returns
    ///
    /// Returns the `Builder` instance for chaining.
    pub fn pool_conn_ttl(mut self, ttl: time::Duration) -> Self {
        self.conn_ttl = Some(ttl);
        self
    }

    /// Sets the connect timeout of connection for the pool.
    ///
    /// # Parameters
    ///
    /// - `timeout`: The connect timeout of connection.
    ///
    /// # Returns
    ///
    /// Returns the `Builder` instance for chaining.
    pub fn pool_connect_timeout(mut self, timeout: time::Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Sets the acquire timeout of connection for the pool.
    ///
    /// # Parameters
    ///
    /// - `timeout`: The acquire timeout of connection.
    ///
    /// # Returns
    ///
    /// Returns the `Builder` instance for chaining.
    pub fn pool_acquire_timeout(mut self, timeout: time::Duration) -> Self {
        self.acquire_timeout = Some(timeout);
        self
    }

    /// Finalizes the builder and constructs the `Client` with the specified configuration.
    ///
    /// This method takes the current configuration of the `Builder` instance, applies defaults
    /// for any unspecified settings, and creates a new `Client` instance using these configurations.
    /// Specifically, it initializes a new connection pool with the configured parameters and
    /// associates this pool with the new `Client`.
    ///
    /// # Returns
    ///
    /// A `Client` instance configured according to the builder's settings.
    pub fn build(self) -> Client {
        let pool = connection::Pool::new(
            self.max_conns.unwrap_or(100),
            self.conn_ttl.unwrap_or(time::Duration::from_secs(120)),
            self.connect_timeout.unwrap_or(time::Duration::from_secs(3)),
            self.acquire_timeout
                .unwrap_or(time::Duration::from_secs(60)),
        );
        let mut client = Client::new(rc::Rc::new(pool));
        client.proxy = self.proxy;
        client.proxy_connect_timeout = self.connect_timeout.unwrap_or(time::Duration::from_secs(3));
        client
    }
}
