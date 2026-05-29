//! Streaming file downloads with live progress tracking.
//!
//! [`Fetch::download`](super::Fetch::download) streams a response body to disk in a background task and returns a
//! [`Download`] handle immediately. Callers read live progress through [`Download::progress`] (a [`Progress`]
//! snapshot), poll [`Download::completed`]/[`Download::failed`], await updates with [`Download::changed`], or await the
//! final [`Result`] with [`Download::join`]. Progress is shared over a [`tokio::sync::watch`] channel — the background
//! task is the single producer, the handle is the observer.

use std::fmt;
use std::path::PathBuf;

use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::watch;

use super::{Fetch, PreparedRequest, RequestOptions, retry};

/// A snapshot of a download's progress, carried over the [`watch`] channel and returned by [`Download::progress`].
#[derive(Debug, Clone, Default)]
pub struct Progress {
    /// Total bytes expected, from the `Content-Length` header. `None` when the server did not advertise a length.
    pub total: Option<u64>,
    /// Bytes written to disk so far.
    pub downloaded: u64,
    /// Fraction complete in `0.0..=1.0`. `None` when [`total`](Progress::total) is unknown.
    pub progress: Option<f64>,
    /// `true` once the transfer has finished — on success **or** failure.
    pub completed: bool,
    /// `true` when the transfer finished with an error.
    pub failed: bool,
}

/// Handle to an in-flight (or finished) download started by [`Fetch::download`](super::Fetch::download).
///
/// The download runs in a background task; this handle observes its progress and final result. Dropping the handle does
/// **not** cancel the download.
pub struct Download {
    rx: watch::Receiver<Progress>,
    handle: tokio::task::JoinHandle<Result<(), DownloadError>>,
}

impl Download {
    /// Assembles a handle from the watch receiver and the spawned task. Used by
    /// [`Fetch::download`](super::Fetch::download).
    pub(super) fn from_parts(
        rx: watch::Receiver<Progress>,
        handle: tokio::task::JoinHandle<Result<(), DownloadError>>,
    ) -> Self {
        Self { rx, handle }
    }

    /// Returns the latest [`Progress`] snapshot (a cheap clone of the watched value).
    pub fn progress(&self) -> Progress {
        self.rx.borrow().clone()
    }

    /// `true` once the transfer has finished, whether it succeeded or failed.
    pub fn completed(&self) -> bool {
        self.rx.borrow().completed
    }

    /// `true` when the transfer finished with an error. Use [`Download::join`] to retrieve the error itself.
    pub fn failed(&self) -> bool {
        self.rx.borrow().failed
    }

    /// Waits for the next progress update.
    ///
    /// # Errors
    ///
    /// Returns an error once the background task has ended and dropped its sender (i.e. there will be no more updates);
    /// the last [`Progress`] remains readable via [`Download::progress`].
    pub async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        self.rx.changed().await
    }

    /// Awaits completion and returns the download's final result. Consumes the handle.
    ///
    /// # Errors
    ///
    /// Returns the [`DownloadError`] that ended the download — an HTTP/transport failure or a disk-write failure.
    ///
    /// # Panics
    ///
    /// Panics if the background task panicked.
    pub async fn join(self) -> Result<(), DownloadError> {
        self.handle.await.expect("download task panicked")
    }
}

/// An error from a streaming download: either an HTTP/transport failure or a failure writing the file to disk.
#[derive(Debug)]
pub enum DownloadError {
    /// The request failed, returned an error status, or the response stream errored.
    Http(reqwest::Error),
    /// Writing the downloaded bytes to disk failed.
    Io(std::io::Error),
}

impl fmt::Display for DownloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DownloadError::Http(err) => write!(f, "download request failed: {err}"),
            DownloadError::Io(err) => write!(f, "writing download to disk failed: {err}"),
        }
    }
}

impl std::error::Error for DownloadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DownloadError::Http(err) => Some(err),
            DownloadError::Io(err) => Some(err),
        }
    }
}

impl From<reqwest::Error> for DownloadError {
    fn from(err: reqwest::Error) -> Self {
        DownloadError::Http(err)
    }
}

impl From<std::io::Error> for DownloadError {
    fn from(err: std::io::Error) -> Self {
        DownloadError::Io(err)
    }
}

/// Computes the completion fraction, clamped to `0.0..=1.0`. `None` when the total is unknown; a known total of zero
/// (an empty file) is reported as fully complete.
fn fraction(total: Option<u64>, downloaded: u64) -> Option<f64> {
    total.map(|t| {
        if t == 0 {
            1.0
        } else {
            (downloaded as f64 / t as f64).min(1.0)
        }
    })
}

/// Drives a download to completion, broadcasting progress over `tx` and returning the final result.
///
/// Called inside the background task spawned by [`Fetch::download`](super::Fetch::download). Setup (`prepare`) happens
/// here so its errors surface through the handle. On return, a final [`Progress`] with `completed = true` (and `failed`
/// reflecting the outcome) is sent.
pub(super) async fn run(
    fetch: Fetch,
    url: Result<reqwest::Url, reqwest::Error>,
    path: PathBuf,
    options: RequestOptions,
    tx: watch::Sender<Progress>,
) -> Result<(), DownloadError> {
    let result = stream_to_file(fetch, url, path, options, &tx).await;
    tx.send_modify(|p| {
        p.completed = true;
        p.failed = result.is_err();
    });
    result
}

/// Resolves the request and streams the response body to `path`, retrying the whole transfer with Fibonacci backoff.
///
/// Each attempt truncates the file and resets the progress counters, so a retry restarts cleanly from byte zero.
async fn stream_to_file(
    fetch: Fetch,
    url: Result<reqwest::Url, reqwest::Error>,
    path: PathBuf,
    options: RequestOptions,
    tx: &watch::Sender<Progress>,
) -> Result<(), DownloadError> {
    let PreparedRequest {
        client,
        url,
        method,
        query,
        body,
        retries,
    } = fetch.prepare(url?, options)?;

    retry::with_fibonacci_backoff(retries, || async {
        let mut file = tokio::fs::File::create(&path).await?;
        tx.send_replace(Progress::default());

        let mut request = client.request(method.clone(), url.clone()).query(&query);
        if let Some(body) = &body {
            request = request.json(body);
        }
        let response = request.send().await?.error_for_status()?;

        let total = response.content_length();
        let mut downloaded: u64 = 0;
        tx.send_replace(Progress {
            total,
            downloaded,
            progress: fraction(total, downloaded),
            completed: false,
            failed: false,
        });

        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
            downloaded += chunk.len() as u64;
            tx.send_replace(Progress {
                total,
                downloaded,
                progress: fraction(total, downloaded),
                completed: false,
                failed: false,
            });
        }
        file.flush().await?;
        Ok::<(), DownloadError>(())
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt as _};
    use tokio::net::{TcpListener, TcpStream};

    /// Reads an HTTP request from `stream` up to the blank line terminating the headers.
    async fn read_request(stream: &mut TcpStream) {
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
    }

    /// Writes a minimal HTTP/1.1 response with a `Content-Length` header.
    async fn write_response(stream: &mut TcpStream, status: &str, body: &str) {
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
    }

    /// Writes a 200 response with no `Content-Length`; the body length is implied by the connection close.
    async fn write_response_no_length(stream: &mut TcpStream, body: &str) {
        let response = format!("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n{body}");
        stream.write_all(response.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
    }

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
}
