//! The `error` module defines the custom error types for the Fibreq library.
//!
//! This module encapsulates various error conditions that can arise while
//! using the Fibreq HTTP client, including network issues, protocol errors,
//! and data processing errors. Leveraging `thiserror` for error representation
//! simplifies error handling and integration with other Rust code.

use std::io;

#[cfg(not(feature = "picodata_tarantool"))]
use tarantool::{error, network::client::tcp};

#[cfg(feature = "picodata_tarantool")]
use picodata_tarantool::system::tarantool::{error, network::client::tcp};

/// Represents all possible errors that can occur while using the Fibreq client.
///
/// Each variant corresponds to a different kind of error condition, including
/// timeouts, protocol support, network issues, and data processing errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid HTTP proxy configuration (never contains credentials).
    #[error("invalid HTTP proxy URL")]
    ProxyConfig,

    #[error("invalid HTTP proxy handshake")]
    ProxyProtocol,

    #[error("HTTP proxy rejected request with status {0}")]
    ProxyStatus(u16),
    /// Occurs when an operation exceeds its allocated time limit.
    #[error("timeout")]
    Timeout,

    /// Occurs when connection is closed by remote.
    #[error("socket_closed")]
    SocketClosed,

    /// Indicates that the specified schema is not supported by the client.
    #[error("unsupported schema")]
    UnsupportedSchema,

    /// Triggered when a URL is provided without a hostname.
    #[error("empty host")]
    EmptyHost,

    /// Represents errors related to TCP operations.
    #[error("tcp: {0}")]
    TCP(#[from] tcp::Error),

    /// Encapsulates errors from the underlying HTTP library.
    #[error("http: {0}")]
    HTTP(http_types::Error),

    /// Related to TLS (Transport Layer Security) issues.
    #[error("tls: {0}")]
    TLS(#[from] async_native_tls::Error),

    /// Tarantool-specific errors, indicating issues within the Tarantool environment.
    #[error("tnt: {0}")]
    TNT(#[from] error::Error),

    /// Errors that occur while parsing URLs.
    #[error("url: {0}")]
    URL(#[from] url::ParseError),

    /// Errors related to processing JSON data.
    #[error("json: {0}")]
    JSON(#[from] serde_json::Error),

    /// Errors related to IO.
    #[error("io: {0}")]
    IO(io::Error),
}
