//! HTTP fetching utilities.
//!
//! This module exposes [`Fetch`], a configurable HTTP request builder. It holds the default configuration (headers,
//! retries, HTTP/2 toggle) and sends requests via [`Fetch::text`], retrying with Fibonacci backoff. Individual
//! requests can override the defaults by passing [`RequestOptions`].

mod request;
mod retry;

pub use request::RequestOptions;

use reqwest::header::HeaderMap;

/// Inserts `key`/`value` into `map`, converting both and panicking on invalid input. Shared by the `header` builder
/// methods on [`Fetch`] and [`RequestOptions`].
///
/// # Panics
///
/// Panics if `key` is not a valid header name or `value` is not a valid header value.
fn insert_header<K, V>(map: &mut HeaderMap, key: K, value: V)
where
    K: TryInto<reqwest::header::HeaderName>,
    K::Error: std::fmt::Debug,
    V: TryInto<reqwest::header::HeaderValue>,
    V::Error: std::fmt::Debug,
{
    let key = key.try_into().expect("invalid header name");
    let value = value.try_into().expect("invalid header value");
    map.insert(key, value);
}

/// A configurable HTTP fetcher, built with a fluent (consuming) builder API.
///
/// ```
/// use rust_sak::fetch::Fetch;
///
/// let fetch = Fetch::new()
///     .header("Accept", "application/json")
///     .retries(3)
///     .disable_http2(true);
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
        insert_header(&mut self.headers, key, value);
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

    /// Sets the HTTP/2 toggle: `true` forces requests over HTTP/1.x, `false` keeps HTTP/2 enabled.
    pub fn disable_http2(mut self, disable: bool) -> Self {
        self.disable_http2 = disable;
        self
    }

    /// Sends a request to `url` and returns the response body as a `String`.
    ///
    /// The struct's headers, retry count, and HTTP/2 setting provide the defaults; any field set on `options` takes
    /// priority for this one request. Headers are merged per-key (request values override struct values, other struct
    /// headers are preserved), query parameters from `options` are appended, and the method defaults to `GET`. The
    /// request is retried up to the resolved number of additional times on failure, with Fibonacci backoff between
    /// attempts (1s, 2s, 3s, 5s, …).
    ///
    /// # Errors
    ///
    /// Returns the last [`reqwest::Error`] if the client cannot be built, every attempt fails, or the response body
    /// is not valid UTF-8 text.
    ///
    /// ```no_run
    /// # async fn run() -> Result<(), reqwest::Error> {
    /// use rust_sak::fetch::{Fetch, RequestOptions};
    ///
    /// let body = Fetch::new()
    ///     .text("https://example.com", RequestOptions::new().query("q", "rust"))
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn text(self, url: impl reqwest::IntoUrl, options: RequestOptions) -> Result<String, reqwest::Error> {
        let mut headers = self.headers;
        for (name, value) in options.headers.iter() {
            headers.insert(name.clone(), value.clone());
        }

        let mut builder = reqwest::Client::builder().default_headers(headers);

        if options.disable_http2.unwrap_or(self.disable_http2) {
            builder = builder.http1_only();
        }

        let client = builder.build()?;
        let url = url.into_url()?;
        let method = options.method.unwrap_or(reqwest::Method::GET);
        let query = options.query;
        let retries = options.retries.unwrap_or(self.retries);

        retry::with_fibonacci_backoff(retries, || async {
            client
                .request(method.clone(), url.clone())
                .query(&query)
                .send()
                .await?
                .error_for_status()?
                .text()
                .await
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
    // These spin up a throwaway local HTTP/1.1 server on an ephemeral port and point `text` at it, so they
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
}
