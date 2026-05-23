# Segment

[![Test Status](https://github.com/irevoire/segment/workflows/Rust/badge.svg?event=push)](https://github.com/irevoire/segment/actions)
[![API](https://docs.rs/segment/badge.svg)](https://docs.rs/segment)

**This crate is an unofficial segment analytics client for Rust used by Meilisearch**
- For additional documentation about **segment** visit <https://segment.com/docs/sources/#server>
- For additional information about **Meilisearch** visit <https://github.com/meilisearch/meilisearch>

## Requirements and features

`segment` 0.3 targets Rust 1.85 or newer and uses the Rust 2024 edition.

The default feature set enables `rustls-tls`, which maps to reqwest 0.13's
`rustls` feature. To use native TLS instead:

```toml
segment = { version = "0.3", default-features = false, features = ["native-tls"] }
```

## Example usage(s)

```rust
use segment::{HttpClient, Client, AutoBatcher, Batcher};
use segment::message::{Track, User};
use serde_json::json;

#[tokio::main(flavor = "current_thread")]
async fn main() -> segment::Result<()> {
    let write_key = "YOUR_WRITE_KEY";

    let client = HttpClient::default();
    let batcher = Batcher::new(None);
    let mut batcher = AutoBatcher::new(client, batcher, write_key);

    // Pretend this is reading off of a queue, a file, or some other data
    // source.
    for i in 0..100 {
        let msg = Track {
            user: User::UserId { user_id: format!("user-{}", i) },
            event: "Example Event".to_owned(),
            properties: json!({
                "foo": format!("bar-{}", i),
            }),
            ..Default::default()
        };

        // An error here indicates a message is too large. In real life, you
        // would probably want to put this message in a deadletter queue or some
        // equivalent.
        batcher.push(msg).await?;
    }

    batcher.flush().await?;
    Ok(())
}
```

or when you want to do struct to struct transformations

```rust
use segment::{HttpClient, Client};
use segment::message::{Track, Message, User};
use serde_json::json;

#[tokio::main(flavor = "current_thread")]
async fn main() -> segment::Result<()> {
    let write_key = "YOUR_WRITE_KEY";

    let client = HttpClient::default();
    client.send(write_key, Message::from(Track {
        user: User::UserId { user_id: "some_user_id".to_owned() },
        event: "Example Event".to_owned(),
        properties: json!({
            "some property": "some value",
            "some other property": "some other value",
        }),
        ..Default::default()
    })).await?;

    Ok(())
}
```

## License

<sup>
Licensed under <a href="LICENSE">MIT license</a>.
</sup>
