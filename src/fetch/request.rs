//! Per-request configuration for [`Fetch::text`](super::Fetch::text), [`Fetch::json`](super::Fetch::json), and
//! [`Fetch::download`](super::Fetch::download).

use reqwest::header::HeaderMap;

use super::DownloadMode;

/// Per-request overrides applied to a single [`Fetch::text`](super::Fetch::text),
/// [`Fetch::json`](super::Fetch::json), or [`Fetch::download`](super::Fetch::download) call, built with the same fluent
/// (consuming) builder API as [`Fetch`](super::Fetch).
///
/// Anything set here takes priority over the [`Fetch`](super::Fetch) struct's own configuration for that one request;
/// anything left unset is inherited from the struct. Headers are merged per-key (request values override struct values,
/// other struct headers are preserved), query parameters are appended, and the method defaults to `GET`. A JSON request
/// body can be attached with [`RequestOptions::body`].
///
/// ```
/// use rust_sak::fetch::RequestOptions;
///
/// let options = RequestOptions::new()
///     .method(reqwest::Method::POST)
///     .query("page", "2")
///     .header("Accept", "application/json")
///     .retries(5)
///     .retry_non_idempotent(true);
/// ```
#[derive(Debug, Clone, Default)]
pub struct RequestOptions {
    /// HTTP method for this request. `None` defaults to `GET`.
    pub(super) method: Option<reqwest::Method>,
    /// Headers applied to this request, merged over the struct's headers.
    pub(super) headers: HeaderMap,
    /// Query parameters appended to the URL, in insertion order.
    pub(super) query: Vec<(String, String)>,
    /// Retry override. `None` inherits the struct's retry count.
    pub(super) retries: Option<u32>,
    /// When `true`, allow automatic retries even for non-idempotent methods (e.g. `POST`/`PATCH`). Defaults to
    /// `false`, so only idempotent methods are retried.
    pub(super) retry_non_idempotent: bool,
    /// JSON request body, serialized eagerly by [`RequestOptions::body`]. `None` sends no body.
    pub(super) body: Option<serde_json::Value>,
    /// Download behavior when a file already exists at the target path (only used by
    /// [`Fetch::download`](super::Fetch::download)). `None` inherits the struct's default.
    pub(super) download_mode: Option<DownloadMode>,
}

impl RequestOptions {
    /// Creates an empty set of options: every field is unset and inherited from the [`Fetch`](super::Fetch) struct.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the HTTP method for this request (e.g. `reqwest::Method::POST`). Defaults to `GET` when unset.
    pub fn method(mut self, method: reqwest::Method) -> Self {
        self.method = Some(method);
        self
    }

    /// Inserts a single header applied only to this request.
    ///
    /// Accepts anything convertible into a header name and value (e.g. `&str`).
    ///
    /// # Panics
    ///
    /// Panics if `key` is not a valid header name or `value` is not a valid header value. This is intended for
    /// statically known headers; for headers built from untrusted input, validate them first and use
    /// [`RequestOptions::headers`].
    pub fn header<K, V>(mut self, key: K, value: V) -> Self
    where
        K: TryInto<reqwest::header::HeaderName>,
        K::Error: std::fmt::Debug,
        V: TryInto<reqwest::header::HeaderValue>,
        V::Error: std::fmt::Debug,
    {
        super::insert_header(&mut self.headers, key, value);
        self
    }

    /// Replaces the full set of per-request headers.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    /// Appends a query parameter to the request URL. Call repeatedly to add multiple parameters.
    pub fn query(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.query.push((key.into(), value.into()));
        self
    }

    /// Overrides the number of retry attempts for this request.
    ///
    /// Note that automatic retries apply only to idempotent methods unless [`RequestOptions::retry_non_idempotent`] is
    /// also set; for a non-idempotent method (e.g. `POST`) without that opt-in, this count is effectively ignored.
    pub fn retries(mut self, retries: u32) -> Self {
        self.retries = Some(retries);
        self
    }

    /// Allows automatic retries for non-idempotent methods (e.g. `POST`/`PATCH`).
    ///
    /// By default, only idempotent methods (`GET`/`HEAD`/`PUT`/`DELETE`/`OPTIONS`/`TRACE`) are retried, because
    /// re-sending a `POST`/`PATCH` that failed *after* the server applied its side effect risks a duplicate write. Set
    /// this to `true` when the endpoint is safe to retry (e.g. idempotent by design or guarded by an idempotency key).
    pub fn retry_non_idempotent(mut self, allow: bool) -> Self {
        self.retry_non_idempotent = allow;
        self
    }

    /// Sets a JSON request body, serialized from `body`. Applied by [`Fetch::text`](super::Fetch::text),
    /// [`Fetch::json`](super::Fetch::json), and [`Fetch::download`](super::Fetch::download), which send it with a
    /// `Content-Type: application/json` header.
    ///
    /// # Panics
    ///
    /// Panics if `body` cannot be serialized to JSON.
    pub fn body<T: serde::Serialize>(mut self, body: T) -> Self {
        self.body = Some(serde_json::to_value(body).expect("request body is not serializable to JSON"));
        self
    }

    /// Sets the [`DownloadMode`] for this [`Fetch::download`](super::Fetch::download) call, overriding the struct's
    /// default. Has no effect on [`Fetch::text`](super::Fetch::text) or [`Fetch::json`](super::Fetch::json). When left
    /// unset, the [`Fetch`](super::Fetch) struct's [`download_mode`](super::Fetch::download_mode) is used.
    pub fn download_mode(mut self, mode: DownloadMode) -> Self {
        self.download_mode = Some(mode);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_unset() {
        let options = RequestOptions::new();
        assert!(options.method.is_none());
        assert!(options.headers.is_empty());
        assert!(options.query.is_empty());
        assert!(options.retries.is_none());
        assert!(!options.retry_non_idempotent);
        assert!(options.body.is_none());
        assert!(options.download_mode.is_none());
    }

    #[test]
    fn builder_sets_fields() {
        let options = RequestOptions::new()
            .method(reqwest::Method::POST)
            .header("Accept", "application/json")
            .query("a", "1")
            .query("b", "2")
            .retries(4)
            .retry_non_idempotent(true)
            .body(serde_json::json!({ "name": "rust" }))
            .download_mode(DownloadMode::Skip);

        assert_eq!(options.method, Some(reqwest::Method::POST));
        assert_eq!(options.headers.get("Accept").unwrap(), "application/json");
        assert_eq!(
            options.query,
            vec![("a".to_string(), "1".to_string()), ("b".to_string(), "2".to_string())]
        );
        assert_eq!(options.retries, Some(4));
        assert!(options.retry_non_idempotent);
        assert_eq!(options.body, Some(serde_json::json!({ "name": "rust" })));
        assert_eq!(options.download_mode, Some(DownloadMode::Skip));
    }

    #[test]
    fn headers_replaces_map() {
        let mut map = HeaderMap::new();
        map.insert("X-Test", "1".parse().unwrap());

        let options = RequestOptions::new().headers(map);
        assert_eq!(options.headers.get("X-Test").unwrap(), "1");
    }
}
