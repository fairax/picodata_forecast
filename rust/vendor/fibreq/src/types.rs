//! Defines common types used across the Fibreq HTTP client library.
//!
//! This module centralizes the definition of types that are used in various parts of the
//! Fibreq library.

use http_types::headers;
use std::collections;

/// Represents a collection of HTTP headers where each header name is associated with its values.
///
/// This type is used for storing headers for HTTP requests and responses where the header names
/// are keys in the `HashMap` and the associated values are a list of strings (since HTTP headers
/// can have multiple values).
pub type Headers = collections::HashMap<headers::HeaderName, headers::HeaderValues>;

/// A borrowed reference variant of `Headers`, used for efficiently passing around references to
/// headers without taking ownership.
///
/// This type is particularly useful for functions that need to inspect or read headers without
/// modifying them, allowing for operations on header data without the overhead of cloning the
/// entire headers collection. It maps borrowed header names to borrowed lists of header values.
pub type RefHeaders<'a> = collections::HashMap<&'a headers::HeaderName, &'a headers::HeaderValues>;
