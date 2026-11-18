//! The `request` module defines structures and functionalities for creating and manipulating HTTP requests.
//!
//! This module provides the `Request` struct for representing HTTP requests, along with a `Builder`
//! pattern for constructing requests with various options like headers, body content, and timeouts.
//! It leverages `http_types` for underlying HTTP elements and offers an ergonomic API for request construction.

use crate::{client, debug, error, response, types, utils};
use http_types::headers;
use http_types::headers::ToHeaderValues;
use std::{collections, fmt, time};

#[cfg(not(feature = "picodata_tarantool"))]
use tarantool::fiber::{self};

#[cfg(feature = "picodata_tarantool")]
use picodata_tarantool::system::tarantool::fiber::{self};

/// Represents an HTTP request, including headers, body, and configuration for timeouts.
#[derive(Debug)]
#[allow(clippy::struct_field_names)]
pub struct Request {
    body: Option<http_types::Body>,
    headers: types::Headers,
    inner: http_types::Request,

    tls_timeout: Option<time::Duration>,
    request_timeout: Option<time::Duration>,
    response_timeout: Option<time::Duration>,
}

impl debug::BodyWrapper for Request {
    fn body_back(&mut self, body: Vec<u8>) {
        self.body = Some(body.into());
    }
}

impl Request {
    /// Creates a new `Request` with the specified HTTP method and URL.
    ///
    /// Automatically sets default headers such as `Host`, `User-Agent`, `Accept`, and `Connection`.
    ///
    /// # Parameters
    ///
    /// - `method`: The HTTP method for the request.
    /// - `url`: The URL to which the request will be sent.
    ///
    /// # Returns
    ///
    /// A new instance of `Request`.
    pub fn new(method: http_types::Method, url: http_types::Url) -> Self {
        let headers = collections::HashMap::from([
            (
                headers::HOST,
                url.host()
                    .unwrap()
                    .to_string()
                    .to_header_values()
                    .unwrap()
                    .collect::<headers::HeaderValues>(),
            ),
            (
                headers::USER_AGENT,
                "fibreq/1.0"
                    .to_header_values()
                    .unwrap()
                    .collect::<headers::HeaderValues>(),
            ),
            (
                headers::ACCEPT,
                "*/*"
                    .to_header_values()
                    .unwrap()
                    .collect::<headers::HeaderValues>(),
            ),
            (
                headers::CONNECTION,
                "Keep-Alive"
                    .to_header_values()
                    .unwrap()
                    .collect::<headers::HeaderValues>(),
            ),
        ]);
        Self {
            headers,
            body: None,
            tls_timeout: None,
            request_timeout: None,
            response_timeout: None,
            inner: http_types::Request::new(method, url),
        }
    }

    /// Splits the `Request` into its constituent parts.
    ///
    /// This method is primarily used internally to pass the request components to the execution engine.
    ///
    /// # Returns
    ///
    /// A tuple containing the `HttpRequest`, headers, body, and configured timeouts.
    #[allow(clippy::type_complexity)]
    pub(crate) fn pieces(
        self,
    ) -> (
        http_types::Request,
        types::Headers,
        Option<http_types::Body>,
        time::Duration,
        time::Duration,
        time::Duration,
    ) {
        (
            self.inner,
            self.headers,
            self.body,
            self.tls_timeout.unwrap_or(time::Duration::from_secs(30)),
            self.request_timeout
                .unwrap_or(time::Duration::from_secs(30)),
            self.response_timeout
                .unwrap_or(time::Duration::from_secs(30)),
        )
    }

    /// Returns the url of the HTTP request.
    ///
    /// # Returns
    ///
    /// The HTTP url code as `&http_types::Url`.
    pub fn url(&self) -> &http_types::Url {
        self.inner.url()
    }

    /// Returns the headers of the HTTP request.
    ///
    /// # Returns
    ///
    /// The HTTP headers as `&types::Headers`.
    #[inline]
    pub fn headers(&self) -> &types::Headers {
        &self.headers
    }

    /// Returns the mutable headers of the HTTP request.
    ///
    /// # Returns
    ///
    /// The HTTP headers as `&mut types::Headers`.
    #[inline]
    pub fn headers_mut(&mut self) -> &mut types::Headers {
        &mut self.headers
    }

    /// Returns the body of the HTTP request.
    ///
    /// # Returns
    ///
    /// The HTTP body as `Option<&http_types::Body>`.
    #[inline]
    pub fn body(&self) -> Option<&http_types::Body> {
        self.body.as_ref()
    }

    /// Returns the mutable body of the HTTP request.
    ///
    /// # Returns
    ///
    /// The HTTP body as `Option<&mut http_types::Body>`.
    #[inline]
    pub fn body_mut(&mut self) -> Option<&mut http_types::Body> {
        self.body.as_mut()
    }

    /// Returns a wrapped version of `Request` body.
    ///
    /// It's binary representation will be brought back to request on [`debug::WrappedBody`] `Drop`.
    ///
    /// # Returns
    ///
    /// A wrapped request body.
    #[inline]
    pub fn wrapped_body(&mut self) -> debug::WrappedBody<'_, Request> {
        match self.body.take() {
            Some(body) => {
                let b =
                    fiber::block_on(body.into_bytes()).map_err(|e| Box::new(error::Error::HTTP(e)));
                debug::WrappedBody::new(self, Some(b))
            }
            None => debug::WrappedBody::new(self, None),
        }
    }
}

/// A builder for constructing instances of `Request`.
///
/// Provides a fluent interface to set various aspects of an HTTP request,
/// including headers, body, authentication, and timeouts. It leverages a
/// `client::Client` for sending the constructed request.
#[derive(Debug)]
pub struct Builder {
    request: Request,
    client: client::Client,
}

impl Builder {
    /// Constructs a new `Builder` with the given `Request` and `Client`.
    ///
    /// This method is internal and intended to be used by the Fibreq library
    /// to initiate request construction.
    ///
    /// # Parameters
    ///
    /// - `request`: The initial `Request` object.
    /// - `client`: The `Client` that will be used to send the request.
    pub(crate) fn new(request: Request, client: client::Client) -> Self {
        Self { request, client }
    }

    /// Sets a single header for the request.
    ///
    /// # Type Parameters
    ///
    /// - `K`: The type of the header name, implementing `Into<headers::HeaderName>`.
    /// - `V`: The type of the header value, implementing `ToHeaderValues`.
    ///
    /// # Parameters
    ///
    /// - `key`: The name of the header.
    /// - `value`: The value of the header.
    ///
    /// # Returns
    ///
    /// Returns `Self` for chaining, or an `Error` if the header value conversion fails.
    ///
    /// # Errors
    ///
    /// Returns an `error::Error::HTTP` in case of failed converting value to header value.
    #[allow(clippy::needless_pass_by_value)]
    pub fn header<K, V>(self, key: K, value: V) -> Result<Self, Box<error::Error>>
    where
        K: Into<headers::HeaderName>,
        V: ToHeaderValues,
    {
        let Self {
            mut request,
            client,
        } = self;
        let value = value
            .to_header_values()
            .map_err(Box::new(error::Error::HTTP))?
            .collect::<headers::HeaderValues>();
        request.headers.insert(key.into(), value);
        Ok(Self { request, client })
    }

    /// Sets multiple headers for the request.
    ///
    /// # Type Parameters
    ///
    /// - `K`: The type of the header name, implementing `Into<headers::HeaderName>`.
    /// - `V`: The type of the header value, implementing `ToHeaderValues`.
    ///
    /// # Parameters
    ///
    /// - `headers`: A collection of header names and values.
    ///
    /// # Returns
    ///
    /// Returns `Self` for chaining, or an `Error` if any header value conversion fails.
    ///
    /// # Errors
    ///
    /// Returns an `error::Error::HTTP` in case of failed converting value to header value.
    pub fn headers<K, V>(
        self,
        headers: collections::HashMap<K, V>,
    ) -> Result<Self, Box<error::Error>>
    where
        K: Into<headers::HeaderName>,
        V: ToHeaderValues,
    {
        let Self {
            mut request,
            client,
        } = self;
        for (key, value) in headers {
            request.headers.insert(
                key.into(),
                value
                    .to_header_values()
                    .map_err(Box::new(error::Error::HTTP))?
                    .collect::<headers::HeaderValues>(),
            );
        }

        Ok(Self { request, client })
    }

    /// Adds basic authentication to the request.
    ///
    /// # Parameters
    ///
    /// - `username`: The username for basic auth.
    /// - `password`: The password for basic auth, optional.
    ///
    /// # Returns
    ///
    /// Returns `Self` for chaining. Panics if setting the header fails.
    pub fn basic_auth<U, P>(self, username: U, password: Option<P>) -> Self
    where
        U: fmt::Display,
        P: fmt::Display,
    {
        self.header(
            headers::AUTHORIZATION,
            utils::basic_auth(username, password),
        )
        .expect("Basic auth header cannot fail.")
    }

    /// Adds bearer token authentication to the request.
    ///
    /// # Parameters
    ///
    /// - `token`: The bearer token.
    ///
    /// # Returns
    ///
    /// Returns `Self` for chaining. Panics if setting the header fails.
    pub fn bearer_auth<T>(self, token: T) -> Self
    where
        T: fmt::Display,
    {
        self.header(headers::AUTHORIZATION, format!("Bearer {token}"))
            .expect("Bearer auth header cannot fail.")
    }

    /// Sets the body of the request.
    ///
    /// # Type Parameters
    ///
    /// - `T`: The type of the body, convertible into `http_types::Body`.
    ///
    /// # Parameters
    ///
    /// - `body`: The body content.
    ///
    /// # Returns
    ///
    /// Returns `Self` for chaining.
    pub fn body<T: Into<http_types::Body>>(self, body: T) -> Self {
        let Self {
            mut request,
            client,
        } = self;
        request.body = Some(body.into());
        Self { request, client }
    }

    /// Adds query parameters to the request URL.
    ///
    /// # Type Parameters
    ///
    /// - `T`: The type of the query parameters, serializable into query string.
    ///
    /// # Parameters
    ///
    /// - `query`: The query parameters.
    ///
    /// # Returns
    ///
    /// Returns `Result<Self, error::Error>`, enabling further chaining of builder methods
    /// or returning an error if serialization fails.
    ///
    /// # Errors
    ///
    /// Returns an `error::Error::HTTP` in case of failed set query.
    pub fn query<T: serde::Serialize>(self, query: &T) -> Result<Self, Box<error::Error>> {
        let Self {
            mut request,
            client,
        } = self;
        request
            .inner
            .set_query(query)
            .map_err(Box::new(error::Error::HTTP))?;
        Ok(Self { request, client })
    }

    /// Sets the request body to a JSON payload.
    ///
    /// Serializes the given object as JSON and sets it as the request body. This method
    /// automatically sets the `Content-Type` header to `application/json`.
    ///
    /// # Type Parameters
    ///
    /// - `T`: The type of the object to serialize into JSON.
    ///
    /// # Parameters
    ///
    /// - `json`: A reference to the object to be serialized.
    ///
    /// # Returns
    ///
    /// Returns `Result<Self, error::Error>`, enabling further chaining of builder methods
    /// or returning an error if serialization fails.
    ///
    /// # Errors
    ///
    /// Returns an `error::Error::HTTP` in case of failed serialization.
    pub fn json<T: serde::Serialize>(self, json: &T) -> Result<Self, Box<error::Error>> {
        let builder = self
            .header(headers::CONTENT_TYPE, "application/json")
            .expect("Json header cannot fail.");
        Ok(builder.body(serde_json::to_vec(json).map_err(|e| Box::new(error::Error::JSON(e)))?))
    }

    /// Sets a custom TLS handshake timeout for the request.
    ///
    /// # Parameters
    ///
    /// - `timeout`: The duration to set as the TLS handshake timeout.
    ///
    /// # Returns
    ///
    /// Returns `Self` for chaining.
    pub fn tls_timeout(self, timeout: time::Duration) -> Self {
        let Self {
            mut request,
            client,
        } = self;
        request.tls_timeout = Some(timeout);
        Self { request, client }
    }

    /// Sets a custom timeout for sending the request.
    ///
    /// # Parameters
    ///
    /// - `timeout`: The duration to set as the request sending timeout.
    ///
    /// # Returns
    ///
    /// Returns `Self` for chaining.
    pub fn request_timeout(self, timeout: time::Duration) -> Self {
        let Self {
            mut request,
            client,
        } = self;
        request.request_timeout = Some(timeout);
        Self { request, client }
    }

    /// Sets a custom timeout for the response read.
    ///
    /// # Parameters
    ///
    /// - `timeout`: The duration to set as the response timeout.
    ///
    /// # Returns
    ///
    /// Returns `Self` for chaining.
    pub fn response_timeout(self, timeout: time::Duration) -> Self {
        let Self {
            mut request,
            client,
        } = self;
        request.response_timeout = Some(timeout);
        Self { request, client }
    }

    /// Finalizes the builder and returns the constructed `Request`.
    ///
    /// # Returns
    ///
    /// The fully constructed `Request` object.
    pub fn build(self) -> Request {
        self.request
    }

    /// Sends the constructed request using the associated client.
    ///
    /// This method executes the request and waits for the response.
    ///
    /// # Returns
    ///
    /// Returns a `Result<response::Response, error::Error>`, containing the response
    /// or an error if the request fails.
    ///
    /// # Errors
    ///
    /// Returns an `error::Error` same as `client::Client::execute`.
    pub fn send(self) -> Result<response::Response, Box<error::Error>> {
        self.client.execute(self.request)
    }
}
