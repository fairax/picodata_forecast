//! Manages TCP connections for the Fibreq HTTP client.
//!
//! This module provides the infrastructure for managing TCP connections, including maintaining
//! a pool of idle connections for reuse, and handling the creation, expiration, and execution
//! of requests over these connections.

use crate::{error, request, response};
use http_types::headers;
use std::{cell, collections, rc, time};

#[cfg(not(feature = "picodata_tarantool"))]
use tarantool::{
    fiber::{
        self,
        r#async::timeout::{self, IntoTimeout},
    },
    network::client::tcp,
    time as ttime,
};

#[cfg(feature = "picodata_tarantool")]
use picodata_tarantool::system::tarantool::{
    fiber::{
        self,
        r#async::timeout::{self, IntoTimeout},
    },
    network::client::tcp,
    time as ttime,
};

/// A mutex-guarded vector of idle connection instances.
type IdleConns = fiber::Mutex<Vec<Inner>>;

/// A hierarchical structure mapping hostnames and ports to their respective connection pools.
type MappedPool =
    fiber::Mutex<collections::HashMap<String, collections::HashMap<u16, rc::Rc<Container>>>>;
/// A queue for tasks waiting for an available connection.
type WaitQueue = fiber::Mutex<collections::LinkedList<fiber::Channel<Inner>>>;

/// Represents an individual TCP connection and its metadata.
#[derive(Debug)]
struct Inner {
    host: String,
    port: u16,

    ttl: time::Duration,
    stream: tcp::TcpStream,
    created: ttime::Instant,
    connect_timeout: time::Duration,
}

impl Inner {
    /// Attempts to create a new TCP connection to the specified host and port.
    ///
    /// # Parameters
    ///
    /// - `host`: The hostname or IP address to connect to.
    /// - `port`: The port number on the host.
    /// - `ttl`: The time-to-live for the connection.
    /// - `connect_timeout`: The timeout duration for establishing the connection.
    ///
    /// # Returns
    ///
    /// A `Result` wrapping the new `Inner` instance, or an `error::Error` on failure.
    ///
    /// # Errors
    ///
    /// Returns an `error::Error::TCP` if the TCP connection cannot be established.
    fn try_new(
        host: String,
        port: u16,
        ttl: time::Duration,
        connect_timeout: time::Duration,
    ) -> Result<Self, Box<error::Error>> {
        let created = ttime::Instant::now_fiber();
        let stream = tcp::TcpStream::connect_timeout(&host, port, connect_timeout)
            .map_err(|e| Box::new(error::Error::TCP(e)))?;
        Ok(Self {
            host,
            port,
            ttl,
            stream,
            created,
            connect_timeout,
        })
    }

    /// Recreates the TCP connection.
    ///
    /// # Returns
    ///
    /// A `Result` wrapping a new `Inner` instance with a fresh connection, or an `error::Error` on failure.
    ///
    /// # Errors
    ///
    /// Returns an `error::Error::TCP` if the TCP connection cannot be re-established.
    fn recreate(self) -> Result<Self, Box<error::Error>> {
        drop(self.stream);
        Self::try_new(self.host, self.port, self.ttl, self.connect_timeout)
    }

    /// Checks whether the connection has exceeded its TTL.
    ///
    /// # Returns
    ///
    /// `true` if the connection is expired, `false` otherwise.
    fn is_expired(&self) -> bool {
        self.created.elapsed() > self.ttl
    }

    /// Executes a given `request::Request` over this connection.
    ///
    /// # Parameters
    ///
    /// - `request`: The request to execute.
    ///
    /// # Returns
    ///
    /// A `Result` wrapping a `response::Response` on success, or an `error::Error` on failure.
    ///
    /// # Errors
    ///
    /// - Returns `error::Error::TLS` for failures related to TLS handshake.
    /// - Returns `error::Error::HTTP` for failures in sending the request or receiving the response.
    /// - Returns `error::Error::Timeout` for operation timeouts.
    async fn execute(
        &self,
        request: request::Request,
    ) -> Result<response::Response, Box<error::Error>> {
        let (mut request, headers, body, tls_timeout, request_timeout, response_timeout) =
            request.pieces();

        // TODO: do something with clone
        for (key, value) in headers {
            request.insert_header(key, &value);
        }

        if let Some(b) = body {
            request.set_body(b);
        }

        let url = request.url().to_owned();

        let stream = self.stream.clone();

        let response = if url.scheme() == "https" {
            let stream = async_native_tls::connect(&self.host, stream)
                .timeout(tls_timeout)
                .await
                .map_err(|x| match x {
                    timeout::Error::Failed(e) => Box::new(error::Error::TLS(e)),
                    timeout::Error::Expired => Box::new(error::Error::Timeout),
                })?;
            async_h1::connect(stream, request)
                .timeout(request_timeout)
                .await
        } else {
            async_h1::connect(stream, request)
                .timeout(request_timeout)
                .await
        }
        .map_err(|x| match x {
            timeout::Error::Failed(e) => Box::new(error::Error::HTTP(e)),
            timeout::Error::Expired => Box::new(error::Error::Timeout),
        })?;

        Ok(response::Response::new(url, response, response_timeout))
    }
}

/// Represents a pool of connections for a specific host and port, including functionality
/// for connection reuse, creation, and lifecycle management.
#[derive(Debug)]
struct Container {
    host: String,
    port: u16,

    max_size: usize,
    current_size: cell::Cell<usize>,

    conn_ttl: time::Duration,
    connect_timeout: time::Duration,
    acquire_timeout: time::Duration,

    idle_conns: IdleConns,
    wait_queue: WaitQueue,
}

impl Container {
    /// Creates a new connection pool container for the specified host and port.
    ///
    /// # Parameters
    ///
    /// - `host`: The hostname or IP address of the target server.
    /// - `port`: The port on the target server.
    /// - `max_size`: The maximum number of connections to maintain in the pool.
    /// - `conn_ttl`: The lifetime of the connection. Connection is dropped when ttl has expired. It helps with busting DNS cache in case of small `TLSes`.
    /// - `connect_timeout`: The timeout for establishing new TCP connections.
    /// - `acquire_timeout`: The timeout for acquiring a connection from the pool.
    ///
    /// # Returns
    ///
    /// A new instance of `Container`.
    fn new(
        host: String,
        port: u16,
        max_size: usize,
        conn_ttl: time::Duration,
        connect_timeout: time::Duration,
        acquire_timeout: time::Duration,
    ) -> Self {
        Self {
            host,
            port,
            max_size,
            conn_ttl,
            current_size: cell::Cell::new(0),
            connect_timeout,
            acquire_timeout,
            idle_conns: fiber::Mutex::new(Vec::with_capacity(max_size)),
            wait_queue: fiber::Mutex::new(collections::LinkedList::new()),
        }
    }

    /// Attempts to acquire a connection from the pool, creating a new connection if necessary
    /// and permissible, or waiting for an available connection if the pool is at its maximum size.
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Inner` connection instance or an `error::Error` if unable to acquire
    /// a connection within the specified constraints.
    ///
    /// # Errors
    ///
    /// Returns `error::Error::Timeout` if unable to acquire a connection within `acquire_timeout`.
    /// May also return other errors related to connection creation failures.
    fn acquire(&self) -> Result<Inner, Box<error::Error>> {
        let mut guard = self.idle_conns.lock();
        if let Some(mut v) = guard.pop() {
            if v.is_expired() {
                v = v.recreate()?;
            }
            return Ok(v);
        }
        // If current size is less than max size we are allowed to create new conn
        if self.current_size.get() < self.max_size {
            self.current_size.set(self.current_size.get() + 1);
            let v = match Inner::try_new(
                self.host.clone(),
                self.port,
                self.conn_ttl,
                self.connect_timeout,
            ) {
                Ok(v) => v,
                Err(e) => {
                    self.current_size.set(self.current_size.get() - 1);
                    return Err(e);
                }
            };
            return Ok(v);
        }
        // Free guard to allow another fibers release connections
        drop(guard);
        let mut guard = self.wait_queue.lock();
        let chan = fiber::Channel::new(1);
        guard.push_back(fiber::Channel::clone(&chan));
        // Free lock to allow another fibers send free connection
        drop(guard);
        let Ok(mut inner) = chan.recv_timeout(self.acquire_timeout) else {
            return Err(Box::new(error::Error::Timeout));
        };
        if inner.is_expired() {
            inner = inner.recreate()?;
        }
        Ok(inner)
    }

    /// Closes a given connection without returning it to the pool, effectively reducing the
    /// pool's current size.
    ///
    /// # Parameters
    ///
    /// - `inner`: The `Inner` connection instance to be closed.
    fn close(&self, inner: Inner) {
        drop(inner);
        self.current_size.set(self.current_size.get() - 1);
    }

    /// Releases a connection back to the pool, or passes it to a waiting fiber if there are
    /// any waiters.
    ///
    /// # Parameters
    ///
    /// - `inner`: The `Inner` connection instance to be released.
    fn release(&self, mut inner: Inner) {
        // First of all lets try to delegate conn impl to waiting fiber
        let mut lock = self.wait_queue.lock();
        while let Some(chan) = lock.pop_front() {
            inner = match chan.send(inner) {
                // Successfully delegated conn impl so we can return
                Ok(()) => return,
                Err(v) => v,
            }
        }
        self.idle_conns.lock().push(inner);
    }
}

/// Represents a high-level HTTP connection capable of executing requests.
///
/// This struct wraps an `Inner` (actual TCP connection) and contains logic to manage
/// its reuse based on HTTP response headers. It is associated with a `Container` that
/// manages a pool of such connections.
#[derive(Debug)]
pub(crate) struct Connection {
    is_reusable: bool,
    inner: Option<Inner>,
    container: rc::Rc<Container>,
}

impl Connection {
    /// Creates a new `Connection` instance from an `Inner` connection and its associated `Container`.
    ///
    /// # Parameters
    ///
    /// - `inner`: The `Inner` instance representing the actual TCP connection.
    /// - `container`: The `Container` that manages the connection pool this connection belongs to.
    ///
    /// # Returns
    ///
    /// A new `Connection` instance.
    fn new(inner: Inner, container: rc::Rc<Container>) -> Self {
        Self {
            container,
            inner: Some(inner),
            is_reusable: false,
        }
    }

    /// Executes a given `request::Request` using this connection.
    ///
    /// Sets the connection to non-reusable if the response contains a `Connection: close` header.
    /// Otherwise, assumes the connection can be reused (`Connection: keep-alive`).
    ///
    /// # Parameters
    ///
    /// - `request`: The HTTP request to execute.
    ///
    /// # Returns
    ///
    /// A `Result` wrapping the `response::Response` or an `error::Error` in case of failure.
    ///
    /// # Errors
    ///
    /// - `error::Error::TLS` if a TLS connection fails.
    /// - `error::Error::HTTP` if sending the request or receiving the response fails.
    /// - `error::Error::Timeout` for operation timeouts.
    pub(crate) fn execute(
        &mut self,
        request: request::Request,
    ) -> Result<response::Response, Box<error::Error>> {
        let result = fiber::block_on(self.inner.as_ref().unwrap().execute(request));

        let response = match result {
            Ok(v) => v,
            Err(e) => {
                if matches!(*e, error::Error::SocketClosed) {
                    self.is_reusable = false;
                }
                return Err(e);
            }
        };

        // If there is close header we down want to bring connection back to pool
        if let Some(header) = response.headers().get(&headers::CONNECTION) {
            let value = header.as_str().to_lowercase();
            self.is_reusable = response.version() == Some(http_types::Version::Http1_1)
                || value.as_str() == "keep-alive";
        }
        Ok(response)
    }
}

impl Drop for Connection {
    /// Handles the dropping of the `Connection` instance.
    ///
    /// Based on the `is_reusable` flag, either releases the connection back to its pool
    /// for reuse or closes it. This ensures that connections are efficiently managed
    /// and resources are not wasted.
    fn drop(&mut self) {
        let inner = self.inner.take().unwrap();
        if self.is_reusable {
            self.container.release(inner);
        } else {
            self.container.close(inner);
        }
    }
}

/// Represents a connection pool manager, capable of providing connections to specified
/// host-port pairs with configurations for connection lifetime, connection and acquisition timeouts.
#[derive(Debug)]
pub(crate) struct Pool {
    /// Max number of connections for one (host, port) pair.
    max_conns: usize,
    /// Time in seconds for connection to be available without underlying `TcpStream` being recreated.
    conn_ttl: time::Duration,
    /// Timeout for `TcpStream` connect.
    connect_timeout: time::Duration,
    /// Timeout for getting connection from pool.
    acquire_timeout: time::Duration,
    /// Actual underlying pool.
    inner: MappedPool,
}

impl Pool {
    /// Creates a new connection pool with specified limits and timeouts.
    ///
    /// # Parameters
    ///
    /// - `max_conns`: The maximum number of simultaneous connections to a single host-port pair.
    /// - `conn_ttl`: The duration before an idle connection is considered expired.
    /// - `connect_timeout`: The timeout duration for establishing new TCP connections.
    /// - `acquire_timeout`: The timeout duration for acquiring a connection from the pool.
    ///
    /// # Returns
    ///
    /// A new instance of `Pool` configured with the specified parameters.
    pub(crate) fn new(
        max_conns: usize,
        conn_ttl: time::Duration,
        connect_timeout: time::Duration,
        acquire_timeout: time::Duration,
    ) -> Self {
        Self {
            max_conns,
            conn_ttl,
            connect_timeout,
            acquire_timeout,
            // Add some initial capacity to avoid reallocations
            inner: fiber::Mutex::new(collections::HashMap::with_capacity(16)),
        }
    }

    /// Retrieves a connection for the given host and port, either by reusing an existing
    /// idle connection or by creating a new one if possible and necessary.
    ///
    /// # Parameters
    ///
    /// - `host`: The hostname or IP address to connect to.
    /// - `port`: The port number on the target host.
    ///
    /// # Returns
    ///
    /// A `Result` wrapping a `Connection` if successful, or an `error::Error` in case of failure.
    ///
    /// # Errors
    ///
    /// - `error::Error::TCP` if there's a failure in establishing a new TCP connection.
    /// - `error::Error::Timeout` if a connection cannot be acquired within `acquire_timeout`.
    pub(crate) fn get(&self, host: &str, port: u16) -> Result<Connection, Box<error::Error>> {
        let mut guard = self.inner.lock();
        if let Some(map) = guard.get_mut(host) {
            if map.get(&port).is_none() {
                map.insert(
                    port,
                    rc::Rc::new(Container::new(
                        host.to_string(),
                        port,
                        self.max_conns,
                        self.conn_ttl,
                        self.connect_timeout,
                        self.acquire_timeout,
                    )),
                );
            }
        } else {
            let mut map = collections::HashMap::with_capacity(16);
            map.insert(
                port,
                rc::Rc::new(Container::new(
                    host.to_string(),
                    port,
                    self.max_conns,
                    self.conn_ttl,
                    self.connect_timeout,
                    self.acquire_timeout,
                )),
            );
            guard.insert(host.to_owned(), map);
        }
        let container = rc::Rc::clone(guard.get(host).unwrap().get(&port).unwrap());
        let inner = container.acquire()?;
        drop(guard);
        Ok(Connection::new(inner, container))
    }
}
