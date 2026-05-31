//! Streaming file downloads with live progress tracking.
//!
//! [`Fetch::download`](super::Fetch::download) streams a response body to disk in a background task and returns a
//! [`Download`] handle immediately. Callers read live progress through [`Download::progress`] (a [`Progress`]
//! snapshot), poll [`Download::completed`]/[`Download::failed`], await updates with [`Download::changed`], or await the
//! final [`Result`] with [`Download::join`]. Progress is shared over a [`tokio::sync::watch`] channel — the background
//! task is the single producer, the handle is the observer.
//!
//! When a file already exists at the target path, [`DownloadMode`] decides the behavior: [`DownloadMode::Resume`] (the
//! default) continues an incomplete transfer via an HTTP `Range` request, [`DownloadMode::Overwrite`] truncates and
//! re-downloads, and [`DownloadMode::Skip`] leaves the existing file untouched.

use std::fmt;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use futures_util::StreamExt;
use reqwest::StatusCode;
use reqwest::header::RANGE;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::watch;

use super::{PreparedRequest, retry};

/// Controls what [`Fetch::download`](super::Fetch::download) does when a file already exists at the target path.
///
/// Set as a struct-wide default with [`Fetch::download_mode`](super::Fetch::download_mode) or per request with
/// [`RequestOptions::download_mode`](super::RequestOptions::download_mode); the per-request value takes priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DownloadMode {
    /// Resume an incomplete file via an HTTP `Range` request, appending the remaining bytes. Falls back to a full
    /// redownload if the server ignores `Range` (responds `200` instead of `206`). Starts fresh when no file exists.
    /// This is the default.
    #[default]
    Resume,
    /// Always truncate any existing file and download from byte zero.
    Overwrite,
    /// If any file already exists at the path, do nothing and report the transfer complete without contacting the
    /// server.
    Skip,
}

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

    /// Invokes `callback` for every progress update until the download finishes, then returns its final result.
    /// Borrows the handle, so the [`Download`] remains usable afterward — e.g. to read the final
    /// [`progress`](Download::progress) snapshot.
    ///
    /// The callback receives `(total, downloaded, progress)` from each [`Progress`] update — the same fields as
    /// [`Progress::total`]/[`Progress::downloaded`]/[`Progress::progress`]. It is **not** called for the initial
    /// zero-valued snapshot (only for updates produced by the transfer) and it **is** called for the final update.
    ///
    /// The background task is awaited exactly once; this method returns its final result, so do **not** call
    /// [`join`](Download::join) afterward (it would re-await a finished task and panic). Reading
    /// [`progress`](Download::progress)/[`completed`](Download::completed)/[`failed`](Download::failed) afterward is
    /// fine.
    ///
    /// # Errors
    ///
    /// Returns the [`DownloadError`] that ended the download — an HTTP/transport failure, a disk-write failure, or
    /// [`DownloadError::Cancelled`] if it was aborted via [`Download::cancel`].
    ///
    /// # Panics
    ///
    /// Panics if the background task panicked.
    pub async fn track<F>(&mut self, mut callback: F) -> Result<(), DownloadError>
    where
        F: FnMut(Option<u64>, u64, Option<f64>),
    {
        while self.rx.changed().await.is_ok() {
            let progress = self.rx.borrow_and_update().clone();
            callback(progress.total, progress.downloaded, progress.progress);
        }
        join_handle(&mut self.handle).await
    }

    /// Cancels the download if it is still running; a no-op if it has already completed or errored.
    ///
    /// Aborts the background task. After cancelling, [`join`](Download::join)/[`track`](Download::track) return
    /// [`DownloadError::Cancelled`]. Aborting mid-transfer leaves the partially written file on disk (no cleanup runs),
    /// so callers that cancel should remove it themselves.
    pub fn cancel(&self) {
        self.handle.abort();
    }

    /// Awaits completion and returns the download's final result. Consumes the handle.
    ///
    /// # Errors
    ///
    /// Returns the [`DownloadError`] that ended the download — an HTTP/transport failure, a disk-write failure, or
    /// [`DownloadError::Cancelled`] if it was aborted via [`Download::cancel`].
    ///
    /// # Panics
    ///
    /// Panics if the background task panicked (a bug), as distinct from being cancelled.
    pub async fn join(mut self) -> Result<(), DownloadError> {
        join_handle(&mut self.handle).await
    }
}

/// Awaits the download task, mapping a cancellation into [`DownloadError::Cancelled`] and re-raising a genuine task
/// panic. Shared by [`Download::join`] and [`Download::track`]. Drives the handle through a mutable borrow (a
/// [`JoinHandle`](tokio::task::JoinHandle) is [`Unpin`]) so `track` can keep the [`Download`] usable afterward.
async fn join_handle(handle: &mut tokio::task::JoinHandle<Result<(), DownloadError>>) -> Result<(), DownloadError> {
    match Pin::new(handle).await {
        Ok(result) => result,
        Err(err) if err.is_cancelled() => Err(DownloadError::Cancelled),
        Err(err) => std::panic::resume_unwind(err.into_panic()),
    }
}

/// An error from a streaming download: an HTTP/transport failure, a failure writing the file to disk, or cancellation.
#[derive(Debug)]
pub enum DownloadError {
    /// The request failed, returned an error status, or the response stream errored.
    Http(reqwest::Error),
    /// Writing the downloaded bytes to the disk failed.
    Io(std::io::Error),
    /// The download was cancelled via [`Download::cancel`] before it finished.
    Cancelled,
}

impl fmt::Display for DownloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DownloadError::Http(err) => write!(f, "download request failed: {err}"),
            DownloadError::Io(err) => write!(f, "writing download to disk failed: {err}"),
            DownloadError::Cancelled => write!(f, "download was cancelled"),
        }
    }
}

impl std::error::Error for DownloadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DownloadError::Http(err) => Some(err),
            DownloadError::Io(err) => Some(err),
            DownloadError::Cancelled => None,
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
/// Called inside the background task spawned by [`Fetch::download`](super::Fetch::download). `prepared` carries any
/// setup error (an invalid URL or a client-build failure) so it surfaces through the handle. `mode` decides how an
/// existing file at `path` is handled. On return, a final [`Progress`] with `completed = true` (and `failed` reflecting
/// the outcome) is sent.
pub(super) async fn run(
    prepared: Result<PreparedRequest, reqwest::Error>,
    path: PathBuf,
    tx: watch::Sender<Progress>,
    mode: DownloadMode,
) -> Result<(), DownloadError> {
    let result = stream_to_file(prepared, path, &tx, mode).await;
    tx.send_modify(|p| {
        p.completed = true;
        p.failed = result.is_err();
    });
    result
}

/// Returns the size of the file at `path`, or `0` when it does not exist (or cannot be stat'd).
async fn file_len(path: &Path) -> u64 {
    tokio::fs::metadata(path).await.map(|m| m.len()).unwrap_or(0)
}

/// Streams the response body to `path`, retrying the whole transfer with Fibonacci backoff.
///
/// The behavior when a file already exists at `path` is governed by `mode`:
/// - [`DownloadMode::Skip`] returns immediately (reporting the existing file as complete) without making a request.
/// - [`DownloadMode::Resume`] sends a `Range` request from the current on-disk length and appends the remainder; if the
///   server ignores `Range` (responds `200`), it falls back to truncating and restarting from byte zero.
/// - [`DownloadMode::Overwrite`] always truncates and downloads from byte zero.
///
/// The offset is re-read from disk at the start of each attempt, so a retry resumes from whatever bytes are already
/// present rather than restarting.
async fn stream_to_file(
    prepared: Result<PreparedRequest, reqwest::Error>,
    path: PathBuf,
    tx: &watch::Sender<Progress>,
    mode: DownloadMode,
) -> Result<(), DownloadError> {
    let prepared = prepared?;

    if mode == DownloadMode::Skip && tokio::fs::try_exists(&path).await? {
        let len = file_len(&path).await;
        tx.send_replace(Progress {
            total: Some(len),
            downloaded: len,
            progress: Some(1.0),
            completed: false,
            failed: false,
        });
        return Ok(());
    }

    retry::with_fibonacci_backoff(prepared.retries, || async {
        // The offset is re-read each attempt, so a retry resumes from whatever is already on disk.
        let offset = match mode {
            DownloadMode::Overwrite => 0,
            DownloadMode::Resume | DownloadMode::Skip => file_len(&path).await,
        };

        let mut builder = prepared.request();
        if offset > 0 {
            builder = builder.header(RANGE, format!("bytes={offset}-"));
        }
        let response = builder.send().await?;

        // A `416` to a ranged request means the offset is at (or past) the end: the file is already complete.
        if offset > 0 && response.status() == StatusCode::RANGE_NOT_SATISFIABLE {
            tx.send_replace(Progress {
                total: Some(offset),
                downloaded: offset,
                progress: Some(1.0),
                completed: false,
                failed: false,
            });
            return Ok(());
        }

        let response = response.error_for_status()?;
        let resuming = offset > 0 && response.status() == StatusCode::PARTIAL_CONTENT;

        // Open the file only after a good response so a failed attempt never leaves a stray empty file (which would
        // corrupt the next attempt's offset).
        let (mut file, mut downloaded, total) = if resuming {
            // A `206` `Content-Length` reports the remaining bytes, so the total is `offset + remaining`.
            let total = response.content_length().map(|remaining| offset + remaining);
            let handle = tokio::fs::OpenOptions::new().append(true).open(&path).await?;
            (BufWriter::new(handle), offset, total)
        } else {
            // A `200` (fresh download, or a server that ignored `Range`): truncate and start from byte zero.
            (
                BufWriter::new(tokio::fs::File::create(&path).await?),
                0,
                response.content_length(),
            )
        };

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
