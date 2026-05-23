//! Utilities for batching up messages.
//! When a batch is full it is automatically sent over the network

use serde_json::Map;

use crate::{
    batcher::Batcher,
    client::Client,
    errors::Result,
    http::HttpClient,
    message::{Batch, BatchMessage, Message},
};

/// A batcher can accept messages into an internal buffer, and report when
/// messages must be flushed.
///
/// The recommended usage pattern looks something like this:
///
/// ```
/// use segment::{AutoBatcher, Batcher, HttpClient};
/// use segment::message::{Track, User};
/// use serde_json::json;
///
/// #[tokio::main(flavor = "current_thread")]
/// async fn main() -> segment::Result<()> {
///     let client = HttpClient::default();
///     let batcher = Batcher::new(None);
///     let mut batcher = AutoBatcher::new(client, batcher, "your_write_key");
///
///     for i in 0..100 {
///         let msg = Track {
///             user: User::UserId { user_id: format!("user-{}", i) },
///             event: "Example".to_owned(),
///             properties: json!({ "foo": "bar" }),
///             ..Default::default()
///         };
///
///         batcher.push(msg).await?;
///     }
///
///     batcher.flush().await?;
///     Ok(())
/// }
/// ```
///
/// Batcher will attempt to fit messages into maximally-sized batches, thus
/// reducing the number of round trips required with Segment's tracking API.
/// However, if you produce messages infrequently, this may significantly delay
/// the sending of messages to Segment.
///
/// If this delay is a concern, it is recommended that you periodically flush
/// the batcher on your own by calling [Self::flush].
#[derive(Clone, Debug)]
pub struct AutoBatcher<C = HttpClient> {
    client: C,
    batcher: Batcher,
    key: String,
}

impl<C> AutoBatcher<C>
where
    C: Client,
{
    /// Construct a new, empty batcher.
    ///
    /// ```
    /// use segment::{AutoBatcher, Batcher, HttpClient};
    ///
    /// let client = HttpClient::default();
    /// let batcher = Batcher::new(None);
    /// let mut batcher = AutoBatcher::new(client, batcher, "your_write_key");
    /// ```
    pub fn new(client: C, batcher: Batcher, key: impl Into<String>) -> Self {
        Self {
            batcher,
            client,
            key: key.into(),
        }
    }

    /// Push a message into the batcher.
    /// If the batcher is full, send it and create a new batcher with the message.
    ///
    /// Returns an error if the message is too large to be sent to Segment's
    /// API.
    ///
    /// ```
    /// use serde_json::json;
    /// use segment::{AutoBatcher, Batcher, HttpClient};
    /// use segment::message::{Track, User};
    ///
    /// #[tokio::main(flavor = "current_thread")]
    /// async fn main() -> segment::Result<()> {
    ///     let client = HttpClient::default();
    ///     let batcher = Batcher::new(None);
    ///     let mut batcher = AutoBatcher::new(client, batcher, "your_write_key");
    ///
    ///     let msg = Track {
    ///         user: User::UserId { user_id: String::from("user") },
    ///         event: "Example".to_owned(),
    ///         properties: json!({ "foo": "bar" }),
    ///         ..Default::default()
    ///     };
    ///
    ///     batcher.push(msg).await?;
    ///     batcher.flush().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn push(&mut self, msg: impl Into<BatchMessage>) -> Result<()> {
        if let Some(msg) = self.batcher.push(msg)? {
            self.flush().await?;
            // this can't return None: the batcher is empty and if the message is
            // larger than the max size of the batcher it's supposed to throw an error
            self.batcher.push(msg)?;
        }

        Ok(())
    }

    /// Send all the message currently contained in the batcher, full or empty.
    ///
    /// Returns an error if the message is too large to be sent to Segment's
    /// API.
    /// ```
    /// use serde_json::json;
    /// use segment::{AutoBatcher, Batcher, HttpClient};
    /// use segment::message::{Track, User};
    ///
    /// #[tokio::main(flavor = "current_thread")]
    /// async fn main() -> segment::Result<()> {
    ///     let client = HttpClient::default();
    ///     let batcher = Batcher::new(None);
    ///     let mut batcher = AutoBatcher::new(client, batcher, "your_write_key");
    ///
    ///     let msg = Track {
    ///         user: User::UserId { user_id: String::from("user") },
    ///         event: "Example".to_owned(),
    ///         properties: json!({ "foo": "bar" }),
    ///         ..Default::default()
    ///     };
    ///
    ///     batcher.push(msg).await?;
    ///     batcher.flush().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn flush(&mut self) -> Result<()> {
        if self.batcher.is_empty() {
            return Ok(());
        }

        let message = Message::Batch(Batch {
            batch: self.batcher.take(),
            context: self.batcher.context.clone(),
            integrations: None,
            extra: Map::default(),
        });

        self.client.send(&self.key, message).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::json;

    use super::*;
    use crate::message::{Track, User};

    #[derive(Clone, Debug)]
    struct RecordingClient {
        sent: Arc<Mutex<Vec<(String, Message)>>>,
    }

    impl Client for RecordingClient {
        fn send<'a>(
            &'a self,
            write_key: &'a str,
            msg: Message,
        ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
            self.sent
                .lock()
                .expect("recording client lock was poisoned")
                .push((write_key.to_owned(), msg));
            std::future::ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn flush_uses_custom_client() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let client = RecordingClient {
            sent: Arc::clone(&sent),
        };
        let mut batcher = AutoBatcher::new(client, Batcher::new(None), "write-key");

        batcher
            .push(Track {
                user: User::UserId {
                    user_id: "user".to_owned(),
                },
                event: "Example".to_owned(),
                properties: json!({ "foo": "bar" }),
                ..Default::default()
            })
            .await
            .expect("message should be accepted");
        batcher.flush().await.expect("batch should flush");

        let sent = sent.lock().expect("recording client lock was poisoned");
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, "write-key");
        let Message::Batch(batch) = &sent[0].1 else {
            panic!("expected a batch message");
        };
        assert_eq!(batch.batch.len(), 1);
    }
}
