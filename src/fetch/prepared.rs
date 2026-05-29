//! The resolved per-request bundle produced by [`Fetch::prepare`](super::Fetch) and consumed by the request methods.

use reqwest::header::HeaderMap;

/// Client and resolved per-request settings produced by [`Fetch::prepare`](super::Fetch), consumed by the request
/// methods. The embedded [`reqwest::Client`] is a cheap clone of the reused client (it shares the underlying
/// connection pool).
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
    /// Bundles the reused client with the fully resolved per-request settings.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        client: reqwest::Client,
        url: reqwest::Url,
        method: reqwest::Method,
        query: Vec<(String, String)>,
        headers: HeaderMap,
        body: Option<serde_json::Value>,
        retries: u32,
    ) -> Self {
        Self {
            client,
            url,
            method,
            query,
            headers,
            body,
            retries,
        }
    }

    /// Assembles the [`reqwest::RequestBuilder`] for one attempt: method, query parameters, per-request header
    /// overrides, and the optional JSON body. Shared by [`Fetch::text`](super::Fetch::text),
    /// [`Fetch::json`](super::Fetch::json), and the download task so the request is built identically everywhere.
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
