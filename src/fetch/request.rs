//! Per-request configuration for [`Fetch::text`](super::Fetch::text).

use reqwest::header::HeaderMap;

/// Per-request overrides applied to a single [`Fetch::text`](super::Fetch::text) call, built with the same fluent
/// (consuming) builder API as [`Fetch`](super::Fetch).
///
/// Anything set here takes priority over the [`Fetch`](super::Fetch) struct's own configuration for that one request;
/// anything left unset is inherited from the struct. Headers are merged per-key (request values override struct
/// values, other struct headers are preserved), query parameters are appended, and the method defaults to `GET`. A
/// JSON request body can be attached with [`RequestOptions::body`].
///
/// ```
/// use rust_sak::fetch::RequestOptions;
///
/// let options = RequestOptions::new()
///     .method(reqwest::Method::POST)
///     .query("page", "2")
///     .header("Accept", "application/json")
///     .retries(5)
///     .disable_http2(true);
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
    /// HTTP/2 toggle override. `None` inherits the struct's setting; `Some(true)` forces HTTP/1.x, `Some(false)`
    /// forces HTTP/2 even if the struct disabled it.
    pub(super) disable_http2: Option<bool>,
    /// JSON request body, serialized eagerly by [`RequestOptions::body`]. `None` sends no body.
    pub(super) body: Option<serde_json::Value>,
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
    pub fn retries(mut self, retries: u32) -> Self {
        self.retries = Some(retries);
        self
    }

    /// Overrides the HTTP/2 setting for this request: `true` forces HTTP/1.x, `false` forces HTTP/2.
    pub fn disable_http2(mut self, disable: bool) -> Self {
        self.disable_http2 = Some(disable);
        self
    }

    /// Sets a JSON request body, serialized from `body`. Applied by both [`Fetch::text`](super::Fetch::text) and
    /// [`Fetch::json`](super::Fetch::json), which send it with a `Content-Type: application/json` header.
    ///
    /// # Panics
    ///
    /// Panics if `body` cannot be serialized to JSON.
    pub fn body<T: serde::Serialize>(mut self, body: T) -> Self {
        self.body = Some(serde_json::to_value(body).expect("request body is not serializable to JSON"));
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
        assert!(options.disable_http2.is_none());
        assert!(options.body.is_none());
    }

    #[test]
    fn builder_sets_fields() {
        let options = RequestOptions::new()
            .method(reqwest::Method::POST)
            .header("Accept", "application/json")
            .query("a", "1")
            .query("b", "2")
            .retries(4)
            .disable_http2(true)
            .body(serde_json::json!({ "name": "rust" }));

        assert_eq!(options.method, Some(reqwest::Method::POST));
        assert_eq!(options.headers.get("Accept").unwrap(), "application/json");
        assert_eq!(
            options.query,
            vec![("a".to_string(), "1".to_string()), ("b".to_string(), "2".to_string())]
        );
        assert_eq!(options.retries, Some(4));
        assert_eq!(options.disable_http2, Some(true));
        assert_eq!(options.body, Some(serde_json::json!({ "name": "rust" })));
    }

    #[test]
    fn disable_http2_records_both_states() {
        assert_eq!(RequestOptions::new().disable_http2(true).disable_http2, Some(true));
        assert_eq!(RequestOptions::new().disable_http2(false).disable_http2, Some(false));
    }

    #[test]
    fn headers_replaces_map() {
        let mut map = HeaderMap::new();
        map.insert("X-Test", "1".parse().unwrap());

        let options = RequestOptions::new().headers(map);
        assert_eq!(options.headers.get("X-Test").unwrap(), "1");
    }
}
