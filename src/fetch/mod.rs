//! HTTP fetching utilities.
//!
//! This module exposes [`Fetch`], a configurable HTTP request builder. It holds the default configuration (headers,
//! retries, HTTP/2 toggle) and sends requests via [`Fetch::text`] (raw body) or [`Fetch::json`] (deserialized into a
//! caller-chosen type), retrying with Fibonacci backoff. [`Fetch::download`] instead streams a response body to a file
//! and returns a [`Download`] handle immediately, exposing live progress as a [`Progress`] snapshot (and surfacing
//! failures as a [`DownloadError`]). Individual requests can override the defaults — including attaching a JSON request
//! body — by passing [`RequestOptions`].

mod download;
mod prepared;
mod request;
mod retry;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

pub use download::{Download, DownloadError, DownloadMode, Progress};
pub use request::RequestOptions;

use std::sync::OnceLock;
use std::time::Duration;

use reqwest::header::HeaderMap;

use prepared::PreparedRequest;

/// A configurable, reusable HTTP fetcher, built with a fluent (consuming) builder API.
///
/// A `Fetch` holds the default configuration (headers, retries, HTTP/2 toggle, read timeout, download mode) and a lazily built,
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
    /// Default [`DownloadMode`] for [`Fetch::download`], overridable per request via
    /// [`RequestOptions::download_mode`]. Defaults to [`DownloadMode::Resume`].
    download_mode: DownloadMode,
    /// Lazily built, reused HTTP client. Cleared by the config builders, so the next request rebuilds it.
    client: OnceLock<reqwest::Client>,
}

impl Default for Fetch {
    /// The default configuration: no headers, no retries, HTTP/2 enabled, a 30-second read (idle) timeout, and
    /// [`DownloadMode::Resume`] for downloads.
    fn default() -> Self {
        Self {
            headers: HeaderMap::new(),
            retries: 0,
            disable_http2: false,
            read_timeout: Some(Duration::from_secs(30)),
            download_mode: DownloadMode::Resume,
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
            download_mode: self.download_mode,
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

    /// Sets the default [`DownloadMode`] for [`Fetch::download`], controlling what happens when a file already exists at
    /// the target path. Defaults to [`DownloadMode::Resume`]. Individual requests can override this via
    /// [`RequestOptions::download_mode`]. Like [`Fetch::retries`], this is not a client-build setting, so it does not
    /// rebuild the cached client.
    ///
    /// ```
    /// use rust_sak::fetch::{DownloadMode, Fetch};
    ///
    /// let fetch = Fetch::new().download_mode(DownloadMode::Overwrite);
    /// ```
    pub fn download_mode(mut self, mode: DownloadMode) -> Self {
        self.download_mode = mode;
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
    /// [`RequestOptions::retry_non_idempotent`] opts in).
    ///
    /// When a file already exists at `path`, the resolved [`DownloadMode`] (the struct's
    /// [`download_mode`](Fetch::download_mode), overridable via [`RequestOptions::download_mode`]) decides the behavior:
    /// [`DownloadMode::Resume`] (the default) continues an incomplete transfer with an HTTP `Range` request — appending
    /// the remaining bytes, or falling back to a full redownload if the server ignores `Range`;
    /// [`DownloadMode::Overwrite`] truncates and re-downloads from byte zero; and [`DownloadMode::Skip`] leaves the
    /// existing file untouched and reports the transfer complete without contacting the server. On a retry the transfer
    /// likewise resumes from whatever bytes are already on disk (under `Resume`) rather than restarting.
    ///
    /// All fallible setup is surfaced through the handle rather than from this call: an invalid URL or a client-build
    /// error is captured and reported, alongside a bad HTTP status, a stream error, or a disk-write error, via
    /// [`Download::failed`]/[`Download::join`] as a [`DownloadError`].
    ///
    /// # Panics
    ///
    /// Must be called from within a Tokio runtime (it spawns a task); panics otherwise.
    ///
    /// The simplest way to follow a download to completion is [`Download::track`], which invokes a callback on every
    /// progress update and resolves with the final [`Result`]:
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
    /// download
    ///     .track(|total, downloaded, progress| {
    ///         match progress {
    ///             Some(fraction) => println!("{:.0}% ({downloaded} bytes)", fraction * 100.0),
    ///             None => println!("{downloaded} bytes (unknown total: {total:?})"),
    ///         }
    ///     })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// For finer control you can instead poll [`Download::progress`] and await [`Download::changed`] in a loop, then
    /// await the final outcome with [`Download::join`].
    pub fn download(
        &self,
        url: impl reqwest::IntoUrl,
        path: impl AsRef<std::path::Path>,
        options: RequestOptions,
    ) -> Download {
        let mode = options.download_mode.unwrap_or(self.download_mode);
        let prepared = self.prepare(url, options);
        let path = path.as_ref().to_path_buf();
        let (tx, rx) = tokio::sync::watch::channel(Progress::default());
        let handle = tokio::spawn(download::run(prepared, path, tx, mode));
        Download::from_parts(rx, handle)
    }
}

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

/// `true` for HTTP methods that are idempotent per RFC 9110 (so safe to retry automatically): `GET`, `HEAD`, `PUT`,
/// `DELETE`, `OPTIONS`, `TRACE`. `POST` and `PATCH` are not.
fn is_idempotent(method: &reqwest::Method) -> bool {
    use reqwest::Method;
    matches!(
        *method,
        Method::GET | Method::HEAD | Method::PUT | Method::DELETE | Method::OPTIONS | Method::TRACE
    )
}
