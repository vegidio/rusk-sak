//! Shared test-only helpers: a throwaway local HTTP/1.1 server used by the `fetch` unit tests.
//!
//! These spin up on an ephemeral port, so the tests exercise the real request path without reaching the network.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Reads an HTTP request from `stream` up to the blank line terminating the headers, returning the raw text.
pub(super) async fn read_request(stream: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];

    loop {
        let n = stream.read(&mut chunk).await.unwrap();
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }

    String::from_utf8_lossy(&buf).into_owned()
}

/// Writes a minimal HTTP/1.1 response with the given status line, body, and a `Content-Length` header.
pub(super) async fn write_response(stream: &mut TcpStream, status: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
}

/// Writes a 200 response with no `Content-Length`; the body length is implied by the connection close.
pub(super) async fn write_response_no_length(stream: &mut TcpStream, body: &str) {
    let response = format!("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n{body}");
    stream.write_all(response.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
}
