//! Scratch binary for playing with the `fetch` module.
//!
//! Run with: `cargo run --features fetch`

use rust_sak::fetch::{Fetch, RequestOptions};

#[tokio::main]
async fn main() -> Result<(), reqwest::Error> {
    let fetch = Fetch::new();

    let body = fetch
        .header("User-Agent", "rust-sak-playground")
        .retries(2)
        .text("https://vinicius.io", RequestOptions::new())
        .await?;

    println!("{body}");

    Ok(())
}
