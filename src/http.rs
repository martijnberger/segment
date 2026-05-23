//! Low-level HTTP bindings to the Segment tracking API.

use crate::Client;
use crate::Message;
use crate::Result;
use std::time::Duration;

/// A client which synchronously sends single messages to the Segment tracking
/// API.
///
/// `HttpClient` implements [`Client`]; see that trait for more on how to send
/// events to Segment.
#[derive(Clone, Debug)]
pub struct HttpClient {
    client: reqwest::Client,
    host: String,
}

impl Default for HttpClient {
    fn default() -> Self {
        HttpClient {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::new(10, 0))
                .build()
                .expect("failed to build default reqwest client"),
            host: "https://api.segment.io".to_owned(),
        }
    }
}

impl HttpClient {
    /// Construct a new `HttpClient` from a `reqwest::Client` and a Segment API
    /// scheme and host.
    ///
    /// If you don't care to re-use an existing `reqwest::Client`, you can use
    /// the `Default::default` value, which will send events to
    /// `https://api.segment.io`.
    pub fn new(client: reqwest::Client, host: impl Into<String>) -> HttpClient {
        HttpClient {
            client,
            host: host.into(),
        }
    }
}

impl Client for HttpClient {
    async fn send<'a>(&'a self, write_key: &'a str, msg: Message) -> Result<()> {
        let path = match msg {
            Message::Identify(_) => "/v1/identify",
            Message::Track(_) => "/v1/track",
            Message::Page(_) => "/v1/page",
            Message::Screen(_) => "/v1/screen",
            Message::Group(_) => "/v1/group",
            Message::Alias(_) => "/v1/alias",
            Message::Batch(_) => "/v1/batch",
        };

        self.client
            .post(format!("{}{}", self.host, path))
            .basic_auth(write_key, Some(""))
            .json(&msg)
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }
}
