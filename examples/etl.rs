//! An example showing how to do an ETL-like operation loading events into
//! Segment.

use segment::message::{Track, User};
use segment::{Batcher, Client, HttpClient};
use serde_json::json;

#[tokio::main(flavor = "current_thread")]
async fn main() -> segment::Result<()> {
    let write_key = "YOUR_WRITE_KEY";

    let client = HttpClient::default();
    let mut batcher = Batcher::new(None);

    // Pretend this is reading off of a queue, a file, or some other data
    // source.
    for i in 0..10 {
        let msg = Track {
            user: User::UserId {
                user_id: format!("user-{}", i),
            },
            event: "Batched Event".to_owned(),
            properties: json!({
                "foo": format!("bar-{}", i),
            }),
            ..Default::default()
        };

        // An error here indicates a message is too large. In real life, you
        // would probably want to put this message in a deadletter queue or some
        // equivalent.
        if let Some(msg) = batcher.push(msg)? {
            client.send(write_key, batcher.into_message()).await?;

            batcher = Batcher::new(None);
            batcher.push(msg)?; // Same error condition as above.
        }
    }

    if !batcher.is_empty() {
        client.send(write_key, batcher.into_message()).await?;
    }

    Ok(())
}
