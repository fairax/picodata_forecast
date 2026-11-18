//! Utility functions for the Fibreq HTTP client.
//!
//! This module provides helper functions that are used internally by the Fibreq library
//! to simplify common tasks.

use base64::{prelude, write};
use std::{fmt, io::Write};

/// Generates a `HeaderValue` suitable for the `Authorization` header using basic authentication.
///
/// This function takes a username and an optional password, encodes them in Base64,
/// and formats them according to the Basic Authentication standard. The resulting
/// `HeaderValue` can be used directly in HTTP request headers to authenticate requests.
///
/// # Parameters
///
/// - `username`: The username as a value implementing the `Display` trait.
/// - `password`: An optional password as a value implementing the `Display` trait.
///
/// # Returns
///
/// Returns a `HeaderValue` representing the encoded authorization value.
pub(crate) fn basic_auth<U, P>(username: U, password: Option<P>) -> http_types::headers::HeaderValue
where
    U: fmt::Display,
    P: fmt::Display,
{
    let mut buf = b"Basic ".to_vec();
    {
        let mut encoder = write::EncoderWriter::new(&mut buf, &prelude::BASE64_STANDARD);
        let _ = write!(encoder, "{username}:",);
        if let Some(password) = password {
            let _ = write!(encoder, "{password}",);
        }
    }

    http_types::headers::HeaderValue::from_bytes(buf).expect("base64 is always valid HeaderValue")
}
