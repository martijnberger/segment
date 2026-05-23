//! An example showing how to do an ETL-like operation loading events into
//! Segment using the `AutoBatcher`.

use segment::message::{Track, User};
use segment::{AutoBatcher, Batcher, HttpClient};
use serde_json::json;

#[tokio::main(flavor = "current_thread")]
async fn main() -> segment::Result<()> {
    let write_key = "YOUR_WRITE_KEY";

    let client = HttpClient::default();
    let batcher = Batcher::new(None);
    let mut batcher = AutoBatcher::new(client, batcher, write_key);

    // Pretend this is reading off of a queue, a file, or some other data
    // source.
    for i in 0..10 {
        let msg = Track {
            user: User::UserId {
                user_id: format!("user-{}", i),
            },
            event: "Auto batched Event".to_owned(),
            properties: json!({
                "foo": format!("bar-{}", i),
            }),
            ..Default::default()
        };

        batcher.push(msg).await?;
    }

    batcher.flush().await?;
    Ok(())
}
