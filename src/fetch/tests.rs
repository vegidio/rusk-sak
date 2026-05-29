use super::*;

#[test]
fn new_has_defaults() {
    let fetch = Fetch::new();
    assert!(fetch.headers.is_empty());
    assert_eq!(fetch.retries, 0);
    assert!(!fetch.disable_http2);
}

#[test]
fn builder_sets_fields() {
    let fetch = Fetch::new()
        .header("Accept", "application/json")
        .retries(3)
        .disable_http2(true);

    assert_eq!(fetch.headers.get("Accept").unwrap(), "application/json");
    assert_eq!(fetch.retries, 3);
    assert!(fetch.disable_http2);
}

#[test]
fn disable_http2_can_re_enable() {
    assert!(!Fetch::new().disable_http2(false).disable_http2);
    assert!(Fetch::new().disable_http2(false).disable_http2(true).disable_http2);
    assert!(!Fetch::new().disable_http2(true).disable_http2(false).disable_http2);
}

#[test]
fn headers_replaces_map() {
    let mut map = HeaderMap::new();
    map.insert("X-Test", "1".parse().unwrap());

    let fetch = Fetch::new().headers(map);
    assert_eq!(fetch.headers.get("X-Test").unwrap(), "1");
}

// --- text tests ---
//
// These point `text` at the throwaway local HTTP/1.1 server in `super::test_support`, so they exercise the
// real request path without reaching the network.

use super::test_support::{read_request, write_response};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

#[tokio::test]
async fn text_returns_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_response(&mut stream, "200 OK", "hello world").await;
    });

    let body = Fetch::new()
        .text(format!("http://{addr}"), RequestOptions::new())
        .await
        .unwrap();

    assert_eq!(body, "hello world");
    server.await.unwrap();
}

#[tokio::test]
async fn text_sends_configured_headers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_request(&mut stream).await;
        write_response(&mut stream, "200 OK", "ok").await;
        request
    });

    let body = Fetch::new()
        .header("X-Custom", "abc123")
        .text(format!("http://{addr}"), RequestOptions::new())
        .await
        .unwrap();

    assert_eq!(body, "ok");
    let request = server.await.unwrap();
    assert!(
        request.to_lowercase().contains("x-custom: abc123"),
        "request was:\n{request}"
    );
}

#[tokio::test]
async fn text_errors_on_failure_status() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_response(&mut stream, "500 Internal Server Error", "nope").await;
    });

    let err = Fetch::new()
        .text(format!("http://{addr}"), RequestOptions::new())
        .await
        .unwrap_err();

    assert_eq!(err.status(), Some(reqwest::StatusCode::INTERNAL_SERVER_ERROR));
    server.await.unwrap();
}

#[tokio::test]
async fn text_retries_until_success() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        // First attempt fails...
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_response(&mut stream, "500 Internal Server Error", "fail").await;
        // ...the retry succeeds.
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_response(&mut stream, "200 OK", "recovered").await;
    });

    let body = Fetch::new()
        .retries(1)
        .text(format!("http://{addr}"), RequestOptions::new())
        .await
        .unwrap();

    assert_eq!(body, "recovered");
    server.await.unwrap();
}

#[tokio::test]
async fn text_appends_query_params() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_request(&mut stream).await;
        write_response(&mut stream, "200 OK", "ok").await;
        request
    });

    let body = Fetch::new()
        .text(
            format!("http://{addr}"),
            RequestOptions::new().query("a", "1").query("b", "2"),
        )
        .await
        .unwrap();

    assert_eq!(body, "ok");
    let request = server.await.unwrap();
    let request_line = request.lines().next().unwrap_or_default();
    assert!(request_line.contains("?a=1&b=2"), "request line was:\n{request_line}");
}

#[tokio::test]
async fn text_uses_per_request_method() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_request(&mut stream).await;
        write_response(&mut stream, "200 OK", "ok").await;
        request
    });

    let body = Fetch::new()
        .text(
            format!("http://{addr}"),
            RequestOptions::new().method(reqwest::Method::POST),
        )
        .await
        .unwrap();

    assert_eq!(body, "ok");
    let request = server.await.unwrap();
    assert!(request.starts_with("POST "), "request was:\n{request}");
}

#[tokio::test]
async fn text_request_header_overrides_struct_header() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_request(&mut stream).await;
        write_response(&mut stream, "200 OK", "ok").await;
        request
    });

    let body = Fetch::new()
        .header("X-Custom", "from-fetch")
        .text(
            format!("http://{addr}"),
            RequestOptions::new().header("X-Custom", "from-request"),
        )
        .await
        .unwrap();

    assert_eq!(body, "ok");
    let request = server.await.unwrap().to_lowercase();
    assert!(request.contains("x-custom: from-request"), "request was:\n{request}");
    assert!(!request.contains("from-fetch"), "request was:\n{request}");
}

#[tokio::test]
async fn text_merges_struct_and_request_headers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_request(&mut stream).await;
        write_response(&mut stream, "200 OK", "ok").await;
        request
    });

    let body = Fetch::new()
        .header("X-From-Fetch", "fetch")
        .text(
            format!("http://{addr}"),
            RequestOptions::new().header("X-From-Request", "request"),
        )
        .await
        .unwrap();

    assert_eq!(body, "ok");
    let request = server.await.unwrap().to_lowercase();
    assert!(request.contains("x-from-fetch: fetch"), "request was:\n{request}");
    assert!(request.contains("x-from-request: request"), "request was:\n{request}");
}

#[tokio::test]
async fn text_per_request_retries_override() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        // First attempt fails...
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_response(&mut stream, "500 Internal Server Error", "fail").await;
        // ...the per-request retry succeeds.
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_response(&mut stream, "200 OK", "recovered").await;
    });

    // The struct disables retries; the per-request override re-enables one.
    let body = Fetch::new()
        .retries(0)
        .text(format!("http://{addr}"), RequestOptions::new().retries(1))
        .await
        .unwrap();

    assert_eq!(body, "recovered");
    server.await.unwrap();
}

#[tokio::test]
async fn post_is_not_retried_by_default() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // The server only ever answers one request, with a failure. If retries were attempted the client would
    // hang waiting for a second response; instead the single failure must surface immediately.
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_response(&mut stream, "500 Internal Server Error", "fail").await;
    });

    let err = Fetch::new()
        .retries(3)
        .text(
            format!("http://{addr}"),
            RequestOptions::new().method(reqwest::Method::POST),
        )
        .await
        .unwrap_err();

    assert_eq!(err.status(), Some(reqwest::StatusCode::INTERNAL_SERVER_ERROR));
    server.await.unwrap();
}

#[tokio::test]
async fn post_is_retried_when_opted_in() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        // First attempt fails...
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_response(&mut stream, "500 Internal Server Error", "fail").await;
        // ...the opted-in retry succeeds.
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_response(&mut stream, "200 OK", "recovered").await;
    });

    let body = Fetch::new()
        .retries(1)
        .text(
            format!("http://{addr}"),
            RequestOptions::new()
                .method(reqwest::Method::POST)
                .retry_non_idempotent(true),
        )
        .await
        .unwrap();

    assert_eq!(body, "recovered");
    server.await.unwrap();
}

#[tokio::test]
async fn read_timeout_errors_on_idle_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // The server accepts and reads the request but never sends a response, leaving the connection idle.
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        tokio::time::sleep(Duration::from_secs(30)).await;
    });

    let err = Fetch::new()
        .read_timeout(Duration::from_millis(200))
        .text(format!("http://{addr}"), RequestOptions::new())
        .await
        .unwrap_err();

    assert!(err.is_timeout(), "expected a timeout error, got: {err}");
}

#[tokio::test]
async fn json_deserializes_body() {
    #[derive(serde::Deserialize)]
    struct Repo {
        name: String,
        stars: u32,
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_response(&mut stream, "200 OK", r#"{"name":"rust-sak","stars":42}"#).await;
    });

    let repo: Repo = Fetch::new()
        .json(format!("http://{addr}"), RequestOptions::new())
        .await
        .unwrap();

    assert_eq!(repo.name, "rust-sak");
    assert_eq!(repo.stars, 42);
    server.await.unwrap();
}

#[tokio::test]
async fn json_errors_on_invalid_json() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_response(&mut stream, "200 OK", "not json").await;
    });

    let result = Fetch::new()
        .json::<serde_json::Value>(format!("http://{addr}"), RequestOptions::new())
        .await;

    assert!(result.is_err());
    server.await.unwrap();
}

#[tokio::test]
async fn text_sends_request_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Read the full request including the body that follows the headers.
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 1024];
        loop {
            let n = stream.read(&mut chunk).await.unwrap();
            buf.extend_from_slice(&chunk[..n]);
            // Once the headers are in, read whatever body bytes arrived with them.
            if n < chunk.len() || buf.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        write_response(&mut stream, "200 OK", "ok").await;
        String::from_utf8_lossy(&buf).into_owned()
    });

    let body = Fetch::new()
        .text(
            format!("http://{addr}"),
            RequestOptions::new()
                .method(reqwest::Method::POST)
                .body(serde_json::json!({ "name": "rust" })),
        )
        .await
        .unwrap();

    assert_eq!(body, "ok");
    let request = server.await.unwrap();
    assert!(
        request.to_lowercase().contains("content-type: application/json"),
        "request was:\n{request}"
    );
    assert!(request.contains(r#"{"name":"rust"}"#), "request was:\n{request}");
}
