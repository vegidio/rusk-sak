//! HTTP fetching utilities.
//!
//! This module exposes [`Fetch`], a configurable HTTP request builder. It holds configuration (headers, retries,
//! HTTP/2 toggle) and can send GET requests via [`Fetch::get_text`], retrying with Fibonacci backoff.

mod retry;

use reqwest::header::HeaderMap;

/// A configurable HTTP fetcher, built with a fluent (consuming) builder API.
///
/// ```
/// use rust_sak::fetch::Fetch;
///
/// let fetch = Fetch::new()
///     .header("Accept", "application/json")
///     .retries(3)
///     .disable_http2();
/// ```
#[derive(Debug, Clone, Default)]
pub struct Fetch {
    /// Headers sent with every request.
    headers: HeaderMap,
    /// Number of times a failed request is retried.
    retries: u32,
    /// When `true`, requests are forced over HTTP/1.x instead of HTTP/2.
    disable_http2: bool,
}

impl Fetch {
    /// Creates a new [`Fetch`] with the default configuration: no headers, no retries, and HTTP/2 enabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a single header.
    ///
    /// Accepts anything convertible into a header name and value (e.g. `&str`).
    ///
    /// # Panics
    ///
    /// Panics if `key` is not a valid header name or `value` is not a valid header value. This is intended for
    /// statically known headers; for headers built from untrusted input, validate them first and use
    /// [`Fetch::headers`].
    pub fn header<K, V>(mut self, key: K, value: V) -> Self
    where
        K: TryInto<reqwest::header::HeaderName>,
        K::Error: std::fmt::Debug,
        V: TryInto<reqwest::header::HeaderValue>,
        V::Error: std::fmt::Debug,
    {
        let key = key.try_into().expect("invalid header name");
        let value = value.try_into().expect("invalid header value");
        self.headers.insert(key, value);
        self
    }

    /// Replaces the full set of headers.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    /// Sets the number of retry attempts for failed requests.
    pub fn retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// Forces requests over HTTP/1.x, disabling HTTP/2.
    pub fn disable_http2(mut self) -> Self {
        self.disable_http2 = true;
        self
    }

    /// Sends a GET request to `url` and returns the response body as a `String`.
    ///
    /// The configured headers and HTTP/2 setting are applied to the client, and the request is retried up to
    /// [`Fetch::retries`] additional times on failure, with Fibonacci backoff between attempts (1s, 2s, 3s, 5s, …).
    ///
    /// # Errors
    ///
    /// Returns the last [`reqwest::Error`] if the client cannot be built, every attempt fails, or the response body
    /// is not valid UTF-8 text.
    ///
    /// ```no_run
    /// # async fn run() -> Result<(), reqwest::Error> {
    /// use rust_sak::fetch::Fetch;
    ///
    /// let body = Fetch::new()
    ///     .get_text("https://example.com")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_text(self, url: impl reqwest::IntoUrl) -> Result<String, reqwest::Error> {
        let mut builder = reqwest::Client::builder().default_headers(self.headers);

        if self.disable_http2 {
            builder = builder.http1_only();
        }

        let client = builder.build()?;
        let url = url.into_url()?;

        retry::with_fibonacci_backoff(self.retries, || async {
            client.get(url.clone()).send().await?.error_for_status()?.text().await
        })
        .await
    }
}

#[cfg(test)]
mod tests {
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
            .disable_http2();

        assert_eq!(fetch.headers.get("Accept").unwrap(), "application/json");
        assert_eq!(fetch.retries, 3);
        assert!(fetch.disable_http2);
    }

    #[test]
    fn headers_replaces_map() {
        let mut map = HeaderMap::new();
        map.insert("X-Test", "1".parse().unwrap());

        let fetch = Fetch::new().headers(map);
        assert_eq!(fetch.headers.get("X-Test").unwrap(), "1");
    }

    // --- get_text tests ---
    //
    // These spin up a throwaway local HTTP/1.1 server on an ephemeral port and point `get_text` at it, so they
    // exercise the real request path without reaching the network.

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    /// Reads an HTTP request from `stream` up to the blank line terminating the headers.
    async fn read_request(stream: &mut TcpStream) -> String {
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

    /// Writes a minimal HTTP/1.1 response with the given status line and body.
    async fn write_response(stream: &mut TcpStream, status: &str, body: &str) {
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
    }

    #[tokio::test]
    async fn get_text_returns_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_request(&mut stream).await;
            write_response(&mut stream, "200 OK", "hello world").await;
        });

        let body = Fetch::new().get_text(format!("http://{addr}")).await.unwrap();

        assert_eq!(body, "hello world");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn get_text_sends_configured_headers() {
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
            .get_text(format!("http://{addr}"))
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
    async fn get_text_errors_on_failure_status() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_request(&mut stream).await;
            write_response(&mut stream, "500 Internal Server Error", "nope").await;
        });

        let err = Fetch::new().get_text(format!("http://{addr}")).await.unwrap_err();

        assert_eq!(err.status(), Some(reqwest::StatusCode::INTERNAL_SERVER_ERROR));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn get_text_retries_until_success() {
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
            .get_text(format!("http://{addr}"))
            .await
            .unwrap();

        assert_eq!(body, "recovered");
        server.await.unwrap();
    }
}
