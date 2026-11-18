//! Provides utils for debugging.

use std::{borrow, fmt};

use crate::error;

const EMPTY_BODY: [u8; 0] = [];

/// A trait for [`WrappedBody`], which allows to set a body back to instance.
pub trait BodyWrapper {
    fn body_back(&mut self, body: Vec<u8>);
}
/// Represents a wrapper for debugging a request/response body.
pub struct WrappedBody<'a, T: BodyWrapper> {
    w: &'a mut T,
    b: Option<Result<Vec<u8>, Box<error::Error>>>,
}

impl<'a, T> WrappedBody<'a, T>
where
    T: BodyWrapper,
{
    /// Create a new wrapped body.
    ///
    /// # Returns
    ///
    /// An instance of `WrappedBody`.
    pub fn new(w: &'a mut T, b: Option<Result<Vec<u8>, Box<error::Error>>>) -> Self {
        Self { w, b }
    }
    /// Get a reference to binary representation of request/response body.
    ///
    /// # Returns
    ///
    /// A byte slice of body.
    pub fn as_slice(&self) -> &[u8] {
        let Some(body) = &self.b else {
            return &EMPTY_BODY;
        };
        let Ok(data) = body else { return &EMPTY_BODY };
        data.as_slice()
    }

    /// Get a borrowed version of request/response body as string.
    ///
    /// # Returns
    ///
    /// A byte borrowed body as string.
    pub fn as_utf8(&self) -> borrow::Cow<'_, str> {
        String::from_utf8_lossy(self.as_slice())
    }
}

impl<T> fmt::Debug for WrappedBody<'_, T>
where
    T: BodyWrapper,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(v) = &self.b {
            match &v {
                Ok(data) => write!(f, "Body(Ok({data:?}))"),
                Err(err) => write!(f, "Body(Err({err:?}))"),
            }
        } else {
            write!(f, "Body(None)")
        }
    }
}

impl<T> Drop for WrappedBody<'_, T>
where
    T: BodyWrapper,
{
    fn drop(&mut self) {
        let Some(body) = self.b.take() else {
            return;
        };
        let Ok(data) = body else { return };
        self.w.body_back(data);
    }
}
