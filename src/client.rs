//! Interfaces to the Segment tracking API.

use std::future::Future;

use crate::{Message, Result};

/// `Client` is a trait representing the HTTP transport layer of the analytics library.
pub trait Client {
    /// Send a single message to Segment using the given write key.
    ///
    /// A `write_key` is an API key for Segment's tracking API. See [Segment's
    /// documentation](https://segment.com/docs/guides/setup/how-do-i-find-my-write-key/)
    /// for how to find this value.
    fn send<'a>(
        &'a self,
        write_key: &'a str,
        msg: Message,
    ) -> impl Future<Output = Result<()>> + Send + 'a;
}
