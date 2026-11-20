//! Provides the `Response` struct for handling HTTP responses.
//!
//! This module defines the `Response` structure and its associated methods, facilitating
//! the handling of HTTP responses, including accessing status codes, headers, and body content.
//! It leverages `http_types` for underlying HTTP elements and provides convenient methods
//! for common response processing tasks.

use crate::{debug, error, types};
use http_types::headers;
use serde::de;
use std::{collections, time};

#[cfg(not(feature = "picodata_tarantool"))]
use tarantool::{fiber, fiber::r#async::timeout};

#[cfg(feature = "picodata_tarantool")]
use picodata_tarantool::system::tarantool::{fiber, fiber::r#async::timeout};

/// Represents an HTTP response received by the Fibreq client.
///
/// This struct encapsulates information about an HTTP response, including the request URL,
/// the underlying HTTP response details, and a timeout for operations like reading the response body.
#[derive(Debug)]
pub struct Response {
    url: http_types::Url,
    inner: http_types::Response,
    timeout: time::Duration,
}

impl debug::BodyWrapper for Response {
    fn body_back(&mut self, body: Vec<u8>) {
        self.inner.set_body(body);
    }
}

impl Response {
    /// Constructs a new `Response`.
    ///
    /// # Parameters
    ///
    /// - `url`: The URL from which the response was received.
    /// - `inner`: The underlying HTTP response object.
    /// - `timeout`: The timeout duration for reading the response body.
    ///
    /// # Returns
    ///
    /// A new instance of `Response`.
    pub(crate) fn new(
        url: http_types::Url,
        inner: http_types::Response,
        timeout: time::Duration,
    ) -> Response {
        Self {
            url,
            inner,
            timeout,
        }
    }

    /// Returns the status code of the HTTP response.
    ///
    /// # Returns
    ///
    /// The HTTP status code as `http_types::StatusCode`.
    #[inline]
    pub fn status(&self) -> http_types::StatusCode {
        self.inner.status()
    }

    /// Returns the HTTP version of the response, if specified.
    ///
    /// # Returns
    ///
    /// The HTTP version as `Option<http_types::Version>`.
    #[inline]
    pub fn version(&self) -> Option<http_types::Version> {
        self.inner.version()
    }

    /// Returns a reference to the headers of the response.
    ///
    /// # Returns
    ///
    /// A `types::RefHeaders` containing references to the response headers.
    #[inline]
    pub fn headers(&self) -> types::RefHeaders<'_> {
        let mut result = collections::HashMap::with_capacity(32);
        for item in self.inner.header_names() {
            if let Some(value) = self.inner.header(item) {
                result.insert(item, value);
            }
        }
        result
    }

    /// Returns the URL from which the response was received.
    ///
    /// # Returns
    ///
    /// A reference to the URL as `&http_types::Url`.
    #[inline]
    pub fn url(&self) -> &http_types::Url {
        &self.url
    }

    /// Attempts to retrieve the content length of the response body.
    ///
    /// # Returns
    ///
    /// An `Option<u64>` representing the content length, if specified in the response headers.
    pub fn content_length(&self) -> Option<u64> {
        self.inner
            .header(headers::CONTENT_LENGTH)
            .map(|x| x.last().as_str().parse::<u64>().ok())?
    }

    /// Provides a reference to any extensions associated with the response.
    ///
    /// # Returns
    ///
    /// A reference to the `http_types::Extensions` of the response.
    pub fn extensions(&self) -> &http_types::Extensions {
        self.inner.ext()
    }

    /// Provides a mutable reference to the response's extensions for modification.
    ///
    /// # Returns
    ///
    /// A mutable reference to the `http_types::Extensions`.
    pub fn extensions_mut(&mut self) -> &mut http_types::Extensions {
        self.inner.ext_mut()
    }

    /// Reads the response body as a `String`.
    ///
    /// # Returns
    ///
    /// A `Result<String, error::Error>` containing the body as a string, or an error if the operation fails.
    ///
    /// # Errors
    ///
    /// - Returns an `error::Error::Timeout` in case of body read timeout.
    /// - Returns an `error::Error::HTTP` in other cases.
    pub fn text(&mut self) -> Result<String, Box<error::Error>> {
        fiber::block_on(timeout::timeout(self.timeout, self.inner.body_string())).map_err(|x| {
            match x {
                timeout::Error::Failed(e) => Box::new(error::Error::HTTP(e)),
                timeout::Error::Expired => Box::new(error::Error::Timeout),
            }
        })
    }

    /// Parses the response body as JSON into a specified type.
    ///
    /// # Type Parameters
    ///
    /// - `T`: The type into which the response body should be deserialized.
    ///
    /// # Returns
    ///
    /// A `Result<T, error::Error>` containing the deserialized type, or an error if the operation fails.
    ///
    /// # Errors
    ///
    /// - Returns an `error::Error::Timeout` in case of body read timeout.
    /// - Returns an `error::Error::HTTP` in other cases.
    pub fn json<T: de::DeserializeOwned>(&mut self) -> Result<T, Box<error::Error>> {
        fiber::block_on(timeout::timeout(self.timeout, self.inner.body_json::<T>())).map_err(|x| {
            match x {
                timeout::Error::Failed(e) => Box::new(error::Error::HTTP(e)),
                timeout::Error::Expired => Box::new(error::Error::Timeout),
            }
        })
    }

    /// Reads the response body as raw bytes.
    ///
    /// # Returns
    ///
    /// A `Result<Vec<u8>, error::Error>` containing the body as bytes, or an error if the operation fails.
    ///
    /// # Errors
    ///
    /// - Returns an `error::Error::Timeout` in case of body read timeout.
    /// - Returns an `error::Error::HTTP` in other cases.
    pub fn bytes(&mut self) -> Result<Vec<u8>, Box<error::Error>> {
        fiber::block_on(timeout::timeout(self.timeout, self.inner.body_bytes())).map_err(
            |x| match x {
                timeout::Error::Failed(e) => Box::new(error::Error::HTTP(e)),
                timeout::Error::Expired => Box::new(error::Error::Timeout),
            },
        )
    }

    /// Parses the response body as form into a specified type.
    ///
    /// # Type Parameters
    ///
    /// - `T`: The type into which the response body should be deserialized.
    ///
    /// # Returns
    ///
    /// A `Result<T, error::Error>` containing the deserialized type, or an error if the operation fails.
    ///
    /// # Errors
    ///
    /// - Returns an `error::Error::Timeout` in case of body read timeout.
    /// - Returns an `error::Error::HTTP` in other cases.
    pub fn form<T: de::DeserializeOwned>(&mut self) -> Result<T, Box<error::Error>> {
        fiber::block_on(timeout::timeout(self.timeout, self.inner.body_form())).map_err(|x| match x
        {
            timeout::Error::Failed(e) => Box::new(error::Error::HTTP(e)),
            timeout::Error::Expired => Box::new(error::Error::Timeout),
        })
    }

    /// Returns a wrapped version of `Response` body.
    ///
    /// It's binary representation will be brought back to response on [`debug::WrappedBody`] `Drop`.
    ///
    /// # Returns
    ///
    /// A wrapped response body.
    #[inline]
    pub fn wrapped_body(&mut self) -> debug::WrappedBody<'_, Response> {
        let b = fiber::block_on(self.inner.take_body().into_bytes())
            .map_err(|e| Box::new(error::Error::HTTP(e)));
        debug::WrappedBody::new(self, Some(b))
    }
}
