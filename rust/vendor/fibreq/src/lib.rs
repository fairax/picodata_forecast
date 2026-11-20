//! # Fibreq (`FIBer` `REQuests`)
//!
//! Fibreq is non-blocking HTTP client designed specifically for the Tarantool ecosystem, leveraging the Tarantool's fiber capabilities.
//! It aims to provide a robust, feature-rich alternative for making HTTP(S) requests directly from Tarantool instances,
//! inspired by the ease of use seen in libraries like `reqwest` but tailored for Tarantool's unique environment.
//!
//! ## Features
//!
//! - **Non-blocking Nature**: Utilizes Tarantool's fiber system for efficient, non-blocking I/O operations.
//! - **Connection Pooling**: Enhances performance by reusing connections across multiple requests, minimizing the overhead of establishing new connections.
//! - **Comprehensive Timeout Controls**: Offers detailed timeout configurations for connection establishment, request sending, and response reading to avoid hanging requests.
//! - **HTTPS Support**: Secures your data with TLS support for HTTPS requests, ensuring your connections are encrypted and safe.
//! - **Flexible Request Building**: Simplifies creating complex requests with a convenient builder pattern, supporting various content types, headers, and body content.
//! - **Error Handling**: Provides clear and actionable errors, making debugging and error resolution straightforward.
//!
//! ## Modules
//!
//! - `request`: Defines the `Request` struct and associated builder for constructing HTTP requests.
//! - `response`: Provides the `Response` struct for accessing data returned by HTTP responses.
//! - `connection`: Manages TCP connections, including a pool for reusing connections.
//! - `error`: Enumerates possible errors that can occur during request preparation and execution.
//! - `types`: Contains type definitions and utility functions used across the library.
//! - `utils`: Offers utility functions for common operations such as encoding basic auth headers.
//! - `debug`: Helper utils for debugging purposes.

#![allow(clippy::must_use_candidate)]
#![allow(clippy::return_self_not_must_use)]
#![allow(clippy::missing_panics_doc)]

mod client;
mod connection;
mod debug;
mod error;
mod request;
mod response;
mod types;
mod utils;
mod proxy;

pub use client::{Builder as ClientBuilder, Client};
pub use error::Error;
pub use request::{Builder as RequestBuilder, Request};
pub use response::Response;
pub use proxy::Proxy;
