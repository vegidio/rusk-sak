//! HTTP fetching utilities.
//!
//! This module exposes [`Fetch`], a configurable HTTP request builder. It holds the default configuration (headers,
//! retries, HTTP/2 toggle) and sends requests via [`Fetch::text`] (raw body) or [`Fetch::json`] (deserialized into a
//! caller-chosen type), retrying with Fibonacci backoff. [`Fetch::download`] instead streams a response body to a file
//! and returns a [`Download`] handle immediately, exposing live progress as a [`Progress`] snapshot (and surfacing
//! failures as a [`DownloadError`]). Individual requests can override the defaults — including attaching a JSON request
//! body — by passing [`RequestOptions`].

mod download;
mod request;
mod retry;

#[cfg(test)]
mod test_support;

pub use download::{Download, DownloadError, Progress};
pub use request::RequestOptions;

use std::sync::OnceLock;
use std::time::Duration;

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

/// A configurable, reusable HTTP fetcher, built with a fluent (consuming) builder API.
///
/// A `Fetch` holds the default configuration (headers, retries, HTTP/2 toggle, read timeout) and a lazily built,
/// reusable [`reqwest::Client`]. The client — and its connection pool — is constructed on the first request and reused
/// across later requests, so a `Fetch` is meant to be configured once and shared (the request methods take `&self`).
/// Mutating the configuration via the builder methods resets the cached client so it is rebuilt with the new settings.
///
/// ```
/// use rust_sak::fetch::Fetch;
///
/// let fetch = Fetch::new()
///     .header("Accept", "application/json")
///     .retries(3)
///     .disable_http2(true);
/// ```
#[derive(Debug)]
pub struct Fetch {
    /// Headers sent with every request.
    headers: HeaderMap,
    /// Number of times a failed request is retried.
    retries: u32,
    /// When `true`, requests are forced over HTTP/1.x instead of HTTP/2.
    disable_http2: bool,
    /// Idle timeout applied per read: a request errors if no data arrives within this window (the timer resets on each
    /// successful read). `None` disables it. Defaults to 30 seconds.
    read_timeout: Option<Duration>,
    /// Lazily built, reused HTTP client. Cleared by the config builders, so the next request rebuilds it.
    client: OnceLock<reqwest::Client>,
}

impl Default for Fetch {
    /// The default configuration: no headers, no retries, HTTP/2 enabled, and a 30-second read (idle) timeout.
    fn default() -> Self {
        Self {
            headers: HeaderMap::new(),
            retries: 0,
            disable_http2: false,
            read_timeout: Some(Duration::from_secs(30)),
            client: OnceLock::new(),
        }
    }
}

impl Clone for Fetch {
    /// Clones the configuration; the cloned `Fetch` starts with an empty client cache (a fresh client is built on its
    /// first request).
    fn clone(&self) -> Self {
        Self {
            headers: self.headers.clone(),
            retries: self.retries,
            disable_http2: self.disable_http2,
            read_timeout: self.read_timeout,
            client: OnceLock::new(),
        }
    }
}

impl Fetch {
    /// Creates a new [`Fetch`] with the default configuration: no headers, no retries, HTTP/2 enabled, and a
    /// 30-second read (idle) timeout.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a single header sent with every request.
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
        self.client = OnceLock::new();
        self
    }

    /// Replaces the full set of headers.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self.client = OnceLock::new();
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
        self.client = OnceLock::new();
        self
    }

    /// Sets the read (idle) timeout applied to every request.
    ///
    /// The timeout is applied to each read operation and **resets after each successful read**, so it bounds how long a
    /// connection may stall without sending data — not the total duration of a request. A slow but steady transfer
    /// (e.g. a large download) never trips it. Pass a [`Duration`] to set it or `None` to disable it. Defaults to 30
    /// seconds.
    ///
    /// ```
    /// use std::time::Duration;
    /// use rust_sak::fetch::Fetch;
    ///
    /// let fetch = Fetch::new().read_timeout(Duration::from_secs(10)); // tighter idle timeout
    /// let patient = Fetch::new().read_timeout(None); // no idle timeout
    /// ```
    pub fn read_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.read_timeout = timeout.into();
        self.client = OnceLock::new();
        self
    }

    /// Returns the cached HTTP client, building it from the current configuration on first use.
    ///
    /// The struct's headers become the client's default headers, the HTTP/2 toggle and read timeout are applied at
    /// build time, and the result is cached for reuse across requests.
    ///
    /// # Errors
    ///
    /// Returns a [`reqwest::Error`] if the client cannot be built.
    fn client(&self) -> Result<&reqwest::Client, reqwest::Error> {
        if let Some(client) = self.client.get() {
            return Ok(client);
        }

        let mut builder = reqwest::Client::builder().default_headers(self.headers.clone());
        if self.disable_http2 {
            builder = builder.http1_only();
        }
        if let Some(timeout) = self.read_timeout {
            builder = builder.read_timeout(timeout);
        }

        let client = builder.build()?;
        Ok(self.client.get_or_init(|| client))
    }

    /// Resolves the per-request settings shared by [`Fetch::text`], [`Fetch::json`], and [`Fetch::download`] against
    /// the reused client.
    ///
    /// The method defaults to `GET`; per-request headers are carried through to be applied at the request level (where
    /// they override the client's default headers per-key); the retry count falls back to the struct's. Automatic
    /// retries are restricted to idempotent methods unless [`RequestOptions::retry_non_idempotent`] opts in, so the
    /// resolved retry count is forced to zero for a non-idempotent method otherwise.
    ///
    /// # Errors
    ///
    /// Returns a [`reqwest::Error`] if the client cannot be built or `url` is invalid.
    fn prepare(&self, url: impl reqwest::IntoUrl, options: RequestOptions) -> Result<PreparedRequest, reqwest::Error> {
        let client = self.client()?.clone();
        let method = options.method.unwrap_or(reqwest::Method::GET);

        let retries = options.retries.unwrap_or(self.retries);
        let retries = if is_idempotent(&method) || options.retry_non_idempotent {
            retries
        } else {
            0
        };

        Ok(PreparedRequest {
            client,
            url: url.into_url()?,
            method,
            query: options.query,
            headers: options.headers,
            body: options.body,
            retries,
        })
    }

    /// Sends a request to `url` and returns the response body as a `String`.
    ///
    /// The struct's headers, retry count, and HTTP/2 setting provide the defaults; any field set on `options` takes
    /// priority for this one request. Headers are merged per-key (request values override struct values, other struct
    /// headers are preserved), query parameters from `options` are appended, the method defaults to `GET`, and a JSON
    /// body set via [`RequestOptions::body`] is attached. The request is retried up to the resolved number of
    /// additional times on failure, with Fibonacci backoff between attempts (1s, 2s, 3s, 5s, …).
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
    pub async fn text(&self, url: impl reqwest::IntoUrl, options: RequestOptions) -> Result<String, reqwest::Error> {
        let prepared = self.prepare(url, options)?;
        retry::with_fibonacci_backoff(prepared.retries, || async {
            prepared.request().send().await?.error_for_status()?.text().await
        })
        .await
    }

    /// Sends a request to `url` and deserializes the JSON response body into `T`.
    ///
    /// Behaves exactly like [`Fetch::text`] — same header merging, query parameters, optional [`RequestOptions::body`],
    /// method default, and Fibonacci-backoff retries — but parses the response body as JSON into any type implementing
    /// [`serde::de::DeserializeOwned`] instead of returning the raw text.
    ///
    /// # Errors
    ///
    /// Returns the last [`reqwest::Error`] if the client cannot be built, every attempt fails, or the response body
    /// cannot be deserialized into `T`.
    ///
    /// ```no_run
    /// # async fn run() -> Result<(), reqwest::Error> {
    /// use rust_sak::fetch::{Fetch, RequestOptions};
    ///
    /// #[derive(serde::Deserialize)]
    /// struct Repo {
    ///     name: String,
    ///     stargazers_count: u32,
    /// }
    ///
    /// let repo: Repo = Fetch::new()
    ///     .header("Accept", "application/json")
    ///     .json("https://api.github.com/repos/rust-lang/rust", RequestOptions::new())
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn json<T: serde::de::DeserializeOwned>(
        &self,
        url: impl reqwest::IntoUrl,
        options: RequestOptions,
    ) -> Result<T, reqwest::Error> {
        let prepared = self.prepare(url, options)?;
        retry::with_fibonacci_backoff(prepared.retries, || async {
            prepared.request().send().await?.error_for_status()?.json::<T>().await
        })
        .await
    }

    /// Streams a request to `url`, writing the response body to `path`, and returns a [`Download`] handle immediately.
    ///
    /// Unlike [`Fetch::text`] and [`Fetch::json`], this does **not** await the transfer: the download runs in a
    /// background task while the body is streamed to disk chunk-by-chunk (never buffered whole in memory). The returned
    /// [`Download`] tracks live progress — total size, bytes downloaded, completion fraction — via
    /// [`Download::progress`], and exposes the final outcome via [`Download::completed`], [`Download::failed`], and
    /// [`Download::join`].
    ///
    /// The struct's headers, retry count, and HTTP/2 setting provide the defaults, with `options` overriding per the
    /// same rules as [`Fetch::text`] (so non-idempotent methods are not retried unless
    /// [`RequestOptions::retry_non_idempotent`] opts in). On a retry the whole transfer restarts: the file is truncated
    /// and re-downloaded from byte zero (there is no `Range`/resume support), so the observed progress briefly resets.
    ///
    /// All fallible setup is surfaced through the handle rather than from this call: an invalid URL or a client-build
    /// error is captured and reported, alongside a bad HTTP status, a stream error, or a disk-write error, via
    /// [`Download::failed`]/[`Download::join`] as a [`DownloadError`].
    ///
    /// # Panics
    ///
    /// Must be called from within a Tokio runtime (it spawns a task); panics otherwise.
    ///
    /// ```no_run
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// use rust_sak::fetch::{Fetch, RequestOptions};
    ///
    /// let mut download = Fetch::new().download(
    ///     "https://example.com/big.bin",
    ///     "/tmp/big.bin",
    ///     RequestOptions::new(),
    /// );
    ///
    /// while !download.completed() {
    ///     let progress = download.progress();
    ///     if let Some(fraction) = progress.progress {
    ///         println!("{:.0}%", fraction * 100.0);
    ///     }
    ///     download.changed().await.ok();
    /// }
    /// download.join().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn download(
        &self,
        url: impl reqwest::IntoUrl,
        path: impl AsRef<std::path::Path>,
        options: RequestOptions,
    ) -> Download {
        let prepared = self.prepare(url, options);
        let path = path.as_ref().to_path_buf();
        let (tx, rx) = tokio::sync::watch::channel(Progress::default());
        let handle = tokio::spawn(download::run(prepared, path, tx));
        Download::from_parts(rx, handle)
    }
}

/// `true` for HTTP methods that are idempotent per RFC 9110 (so safe to retry automatically): `GET`, `HEAD`, `PUT`,
/// `DELETE`, `OPTIONS`, `TRACE`. `POST` and `PATCH` are not.
fn is_idempotent(method: &reqwest::Method) -> bool {
    use reqwest::Method;
    matches!(
        *method,
        Method::GET | Method::HEAD | Method::PUT | Method::DELETE | Method::OPTIONS | Method::TRACE
    )
}

/// Client and resolved per-request settings produced by [`Fetch::prepare`], consumed by the request methods. The
/// embedded [`reqwest::Client`] is a cheap clone of the reused client (it shares the underlying connection pool).
pub(super) struct PreparedRequest {
    client: reqwest::Client,
    url: reqwest::Url,
    method: reqwest::Method,
    query: Vec<(String, String)>,
    /// Per-request headers, applied at the request level so they override the client's default headers per-key.
    headers: HeaderMap,
    body: Option<serde_json::Value>,
    pub(super) retries: u32,
}

impl PreparedRequest {
    /// Assembles the [`reqwest::RequestBuilder`] for one attempt: method, query parameters, per-request header
    /// overrides, and the optional JSON body. Shared by [`Fetch::text`], [`Fetch::json`], and the download task so the
    /// request is built identically everywhere.
    pub(super) fn request(&self) -> reqwest::RequestBuilder {
        let mut request = self
            .client
            .request(self.method.clone(), self.url.clone())
            .query(&self.query)
            .headers(self.headers.clone());
        if let Some(body) = &self.body {
            request = request.json(body);
        }
        request
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
}
