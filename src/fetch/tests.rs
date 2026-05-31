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

use super::test_support::{
    read_request, write_partial_response, write_range_not_satisfiable, write_response, write_response_no_length,
};
use std::path::PathBuf;
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

// --- download tests ---
//
// Like the `text` tests, these point `download` at the throwaway local HTTP/1.1 server in `super::test_support`.

/// A unique temp path for a test, keyed by the (unique) ephemeral port the test bound.
fn temp_path(port: u16) -> PathBuf {
    std::env::temp_dir().join(format!("rust-sak-dl-{port}.bin"))
}

/// Drains progress updates until the background task drops its sender, then returns the final snapshot.
async fn drain(download: &mut Download) -> Progress {
    while download.changed().await.is_ok() {}
    download.progress()
}

#[tokio::test]
async fn download_writes_file() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let path = temp_path(addr.port());

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_response(&mut stream, "200 OK", "hello download").await;
    });

    let download = Fetch::new().download(format!("http://{addr}"), &path, RequestOptions::new());
    download.join().await.unwrap();
    server.await.unwrap();

    let contents = tokio::fs::read(&path).await.unwrap();
    assert_eq!(contents, b"hello download");

    let _ = tokio::fs::remove_file(&path).await;
}

#[tokio::test]
async fn download_reports_total_and_completes_to_full() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let path = temp_path(addr.port());

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_response(&mut stream, "200 OK", "0123456789").await;
    });

    let mut download = Fetch::new().download(format!("http://{addr}"), &path, RequestOptions::new());
    let progress = drain(&mut download).await;
    server.await.unwrap();

    assert!(progress.completed);
    assert!(!progress.failed);
    assert_eq!(progress.total, Some(10));
    assert_eq!(progress.downloaded, 10);
    assert_eq!(progress.progress, Some(1.0));

    let _ = tokio::fs::remove_file(&path).await;
}

#[tokio::test]
async fn download_without_content_length() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let path = temp_path(addr.port());

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_response_no_length(&mut stream, "no length body").await;
    });

    let mut download = Fetch::new().download(format!("http://{addr}"), &path, RequestOptions::new());
    let progress = drain(&mut download).await;
    server.await.unwrap();

    assert!(progress.completed);
    assert!(!progress.failed);
    assert_eq!(progress.total, None);
    assert_eq!(progress.progress, None);

    let contents = tokio::fs::read(&path).await.unwrap();
    assert_eq!(contents, b"no length body");

    let _ = tokio::fs::remove_file(&path).await;
}

#[tokio::test]
async fn download_failed_status_sets_failed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let path = temp_path(addr.port());

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_response(&mut stream, "500 Internal Server Error", "nope").await;
    });

    let mut download = Fetch::new().download(format!("http://{addr}"), &path, RequestOptions::new());
    while download.changed().await.is_ok() {}
    assert!(download.completed());
    assert!(download.failed());

    let err = download.join().await.unwrap_err();
    assert!(matches!(err, DownloadError::Http(_)), "unexpected error: {err}");
    server.await.unwrap();

    let _ = tokio::fs::remove_file(&path).await;
}

#[tokio::test]
async fn track_reports_progress_and_completes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let path = temp_path(addr.port());

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_response(&mut stream, "200 OK", "hello track").await;
    });

    let mut download = Fetch::new().download(format!("http://{addr}"), &path, RequestOptions::new());
    let mut ticks: Vec<(Option<u64>, u64, Option<f64>)> = Vec::new();
    download
        .track(|total, downloaded, progress| ticks.push((total, downloaded, progress)))
        .await
        .unwrap();
    server.await.unwrap();

    assert!(!ticks.is_empty(), "callback was never invoked");
    // The final update reports the full transfer.
    assert_eq!(ticks.last().copied(), Some((Some(11), 11, Some(1.0))));

    let _ = tokio::fs::remove_file(&path).await;
}

#[tokio::test]
async fn track_returns_error_on_failed_status() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let path = temp_path(addr.port());

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_response(&mut stream, "500 Internal Server Error", "nope").await;
    });

    let mut download = Fetch::new().download(format!("http://{addr}"), &path, RequestOptions::new());
    let err = download.track(|_, _, _| {}).await.unwrap_err();
    assert!(matches!(err, DownloadError::Http(_)), "unexpected error: {err}");
    server.await.unwrap();

    let _ = tokio::fs::remove_file(&path).await;
}

#[tokio::test]
async fn cancel_aborts_and_join_returns_cancelled() {
    use tokio::io::AsyncWriteExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let path = temp_path(addr.port());

    // The server advertises a large body but sends only a few bytes, then holds the connection open — so the
    // download stays in-flight (it never errors and never completes) until we cancel it.
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\nhello")
            .await
            .unwrap();
        stream.flush().await.unwrap();
        // Keep the stream alive so the body never finishes; the test aborts this task at the end.
        std::future::pending::<()>().await;
    });

    let mut download = Fetch::new().download(format!("http://{addr}"), &path, RequestOptions::new());
    // Wait for the first real progress update so the transfer is genuinely in-flight before cancelling.
    download.changed().await.unwrap();
    assert!(!download.completed());

    download.cancel();
    let err = download.join().await.unwrap_err();
    assert!(matches!(err, DownloadError::Cancelled), "unexpected error: {err}");

    server.abort();
    let _ = tokio::fs::remove_file(&path).await;
}

#[tokio::test]
async fn download_retries_until_success() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let path = temp_path(addr.port());

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

    let download = Fetch::new().download(format!("http://{addr}"), &path, RequestOptions::new().retries(1));
    download.join().await.unwrap();
    server.await.unwrap();

    let contents = tokio::fs::read(&path).await.unwrap();
    assert_eq!(contents, b"recovered");

    let _ = tokio::fs::remove_file(&path).await;
}

#[tokio::test]
async fn resume_appends_from_offset() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let path = temp_path(addr.port());

    // Pre-seed a partial file: the first 4 bytes of "0123456789".
    let _ = tokio::fs::remove_file(&path).await;
    tokio::fs::write(&path, b"0123").await.unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_request(&mut stream).await;
        assert!(
            request.contains("bytes=4-"),
            "expected a resume Range header, got: {request}"
        );
        write_partial_response(&mut stream, 4, 10, "456789").await;
    });

    let mut download = Fetch::new().download(format!("http://{addr}"), &path, RequestOptions::new());
    let progress = drain(&mut download).await;
    server.await.unwrap();

    assert!(progress.completed);
    assert!(!progress.failed);
    assert_eq!(progress.total, Some(10));
    assert_eq!(progress.downloaded, 10);

    let contents = tokio::fs::read(&path).await.unwrap();
    assert_eq!(contents, b"0123456789");

    let _ = tokio::fs::remove_file(&path).await;
}

#[tokio::test]
async fn resume_falls_back_when_server_ignores_range() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let path = temp_path(addr.port());

    // Pre-seed stale partial bytes; the server ignores Range and replies with a full 200 body.
    let _ = tokio::fs::remove_file(&path).await;
    tokio::fs::write(&path, b"stale").await.unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_response(&mut stream, "200 OK", "full body").await;
    });

    let mut download = Fetch::new().download(format!("http://{addr}"), &path, RequestOptions::new());
    let progress = drain(&mut download).await;
    server.await.unwrap();

    assert!(progress.completed);
    assert!(!progress.failed);

    // The stale partial was truncated, not appended to.
    let contents = tokio::fs::read(&path).await.unwrap();
    assert_eq!(contents, b"full body");

    let _ = tokio::fs::remove_file(&path).await;
}

#[tokio::test]
async fn skip_when_file_exists() {
    // Bind a listener to reserve a port but never accept: Skip must not make a request.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let path = temp_path(addr.port());

    let _ = tokio::fs::remove_file(&path).await;
    tokio::fs::write(&path, b"existing").await.unwrap();

    let mut download = Fetch::new().download(
        format!("http://{addr}"),
        &path,
        RequestOptions::new().download_mode(DownloadMode::Skip),
    );
    let progress = drain(&mut download).await;

    assert!(progress.completed);
    assert!(!progress.failed);
    assert_eq!(progress.downloaded, 8);
    assert_eq!(progress.total, Some(8));

    // The existing file is untouched.
    let contents = tokio::fs::read(&path).await.unwrap();
    assert_eq!(contents, b"existing");

    drop(listener);
    let _ = tokio::fs::remove_file(&path).await;
}

#[tokio::test]
async fn overwrite_truncates_existing() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let path = temp_path(addr.port());

    let _ = tokio::fs::remove_file(&path).await;
    tokio::fs::write(&path, b"old stale contents").await.unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_request(&mut stream).await;
        assert!(
            !request.to_lowercase().contains("range:"),
            "Overwrite must not send a Range header"
        );
        write_response(&mut stream, "200 OK", "fresh").await;
    });

    let mut download = Fetch::new().download(
        format!("http://{addr}"),
        &path,
        RequestOptions::new().download_mode(DownloadMode::Overwrite),
    );
    let progress = drain(&mut download).await;
    server.await.unwrap();

    assert!(progress.completed);
    assert!(!progress.failed);

    let contents = tokio::fs::read(&path).await.unwrap();
    assert_eq!(contents, b"fresh");

    let _ = tokio::fs::remove_file(&path).await;
}

#[tokio::test]
async fn resume_416_treated_as_complete() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let path = temp_path(addr.port());

    // A file that is already complete; the server rejects the range with 416.
    let _ = tokio::fs::remove_file(&path).await;
    tokio::fs::write(&path, b"0123456789").await.unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_range_not_satisfiable(&mut stream, 10).await;
    });

    let mut download = Fetch::new().download(format!("http://{addr}"), &path, RequestOptions::new());
    let progress = drain(&mut download).await;
    server.await.unwrap();

    assert!(progress.completed);
    assert!(!progress.failed);
    assert_eq!(progress.downloaded, 10);

    // The complete file is preserved.
    let contents = tokio::fs::read(&path).await.unwrap();
    assert_eq!(contents, b"0123456789");

    let _ = tokio::fs::remove_file(&path).await;
}

#[tokio::test]
async fn struct_download_mode_default_is_overridable() {
    // The struct default (Overwrite) applies when the request leaves the mode unset...
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let path = temp_path(addr.port());

    let _ = tokio::fs::remove_file(&path).await;
    tokio::fs::write(&path, b"stale").await.unwrap();

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        read_request(&mut stream).await;
        write_response(&mut stream, "200 OK", "fresh").await;
    });

    let fetch = Fetch::new().download_mode(DownloadMode::Overwrite);
    let mut download = fetch.download(format!("http://{addr}"), &path, RequestOptions::new());
    drain(&mut download).await;
    server.await.unwrap();
    assert_eq!(tokio::fs::read(&path).await.unwrap(), b"fresh");

    // ...and a per-request override (Skip) wins over the struct default.
    let _ = tokio::fs::remove_file(&path).await;
    tokio::fs::write(&path, b"kept").await.unwrap();
    let mut download = fetch.download(
        format!("http://{addr}"),
        &path,
        RequestOptions::new().download_mode(DownloadMode::Skip),
    );
    let progress = drain(&mut download).await;
    assert!(progress.completed && !progress.failed);
    assert_eq!(tokio::fs::read(&path).await.unwrap(), b"kept");

    let _ = tokio::fs::remove_file(&path).await;
}
