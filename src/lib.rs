//! Cloud storage copy utility.
//!
//! The `cloud-copy` crate offers a simple API for transferring files to and
//! from Azure Blob Storage, Amazon S3, and Google Cloud Storage.
//!
//! It exports a function named [`copy`] which is responsible for copying a
//! source to a destination.
//!
//! An optional transfer event stream provided to the [`copy`] function can be
//! used to display transfer progress.
//!
//! Additionally, when this crate is built with the `cli` feature enabled, a
//! [`handle_events`][cli::handle_events] function is exported that will display
//! progress bars using the `tracing_indicatif` crate.

#![deny(rustdoc::broken_intra_doc_links)]
#![cfg_attr(docsrs, feature(doc_cfg))]

use std::fmt;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use http_cache_stream_reqwest::Cache;
use http_cache_stream_reqwest::storage::DefaultCacheStorage;
use reqwest::Client;
use reqwest::StatusCode;
use reqwest_middleware::ClientBuilder;
use reqwest_middleware::ClientWithMiddleware;
use tokio::fs::OpenOptions;
use tokio::io::BufReader;
use tokio::io::BufWriter;
use tokio::sync::broadcast;
use tokio_retry2::RetryError;
use tokio_util::io::ReaderStream;
use tokio_util::io::StreamReader;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing::warn;
use url::Url;

use crate::backend::StorageBackend;
use crate::backend::azure::AzureBlobStorageBackend;
use crate::backend::generic::GenericStorageBackend;
use crate::backend::google::GoogleError;
use crate::backend::google::GoogleStorageBackend;
use crate::backend::s3::S3StorageBackend;
use crate::streams::TransferStream;
use crate::transfer::FileTransfer;

mod backend;
#[cfg(feature = "cli")]
#[cfg_attr(docsrs, doc(cfg(feature = "cli")))]
pub mod cli;
mod config;
mod generator;
mod os;
mod pool;
mod streams;
mod transfer;

pub use backend::azure::AzureError;
pub use backend::s3::S3Error;
pub use config::*;
pub use generator::*;

/// The utility user agent.
const USER_AGENT: &str = concat!(
    "cloud-copy/",
    env!("CARGO_PKG_VERSION"),
    " (https://github.com/stjude-rust-labs/cloud-copy)"
);

/// Represents one mebibyte in bytes.
const ONE_MEBIBYTE: u64 = 1024 * 1024;

/// The threshold for which block size calculation uses to minimize the block
/// size (256 MiB).
const BLOCK_SIZE_THRESHOLD: u64 = 256 * ONE_MEBIBYTE;

/// Helper for notifying that a network operation failed and will be retried.
fn notify_retry(e: &Error, duration: Duration) {
    // Duration of 0 indicates the first attempt; only print the message for a retry
    if !duration.is_zero() {
        let secs = duration.as_secs();
        warn!(
            "network operation failed (retried after waiting {secs} second{s}): {e}",
            s = if secs == 1 { "" } else { "s" }
        );
    }
}

/// Represents either a local or remote location.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Location<'a> {
    /// The location is a local path.
    Path(&'a Path),
    /// The location is a URL.
    Url(Url),
}

impl<'a> Location<'a> {
    /// Constructs a new location from a string.
    pub fn new(s: &'a str) -> Self {
        match s.parse::<Url>() {
            Ok(url) => Self::Url(url),
            Err(_) => Self::Path(Path::new(s)),
        }
    }
}

impl fmt::Display for Location<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Path(path) => write!(f, "{path}", path = path.display()),
            Self::Url(url) => write!(f, "{url}", url = url.display()),
        }
    }
}

impl<'a> From<&'a str> for Location<'a> {
    fn from(value: &'a str) -> Self {
        Self::new(value)
    }
}

impl<'a> From<&'a String> for Location<'a> {
    fn from(value: &'a String) -> Self {
        Self::new(value)
    }
}

impl<'a> From<&'a Path> for Location<'a> {
    fn from(value: &'a Path) -> Self {
        Self::Path(value)
    }
}

impl<'a> From<&'a PathBuf> for Location<'a> {
    fn from(value: &'a PathBuf) -> Self {
        Self::Path(value.as_path())
    }
}

impl From<Url> for Location<'_> {
    fn from(value: Url) -> Self {
        Self::Url(value)
    }
}

/// Extension trait for `Url`.
pub trait UrlExt {
    /// Converts the URL to a local path if it uses the `file` scheme.
    ///
    /// Returns `Ok(None)` if the URL is not a `file` scheme.
    ///
    /// Returns an error if the URL uses a `file` scheme but cannot be
    /// represented as a local path.
    fn to_local_path(&self) -> Result<Option<PathBuf>>;

    /// Displays a URL without its query parameters.
    ///
    /// This is used to prevent authentication information from being displayed
    /// to users.
    fn display(&self) -> impl fmt::Display;
}

impl UrlExt for Url {
    fn to_local_path(&self) -> Result<Option<PathBuf>> {
        if self.scheme() != "file" {
            return Ok(None);
        }

        self.to_file_path()
            .map(Some)
            .map_err(|_| Error::InvalidFileUrl(self.clone()))
    }

    fn display(&self) -> impl fmt::Display {
        /// Utility for displaying URLs without query parameters.
        struct Display<'a>(&'a Url);

        impl fmt::Display for Display<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(
                    f,
                    "{scheme}://{host}{path}",
                    scheme = self.0.scheme(),
                    host = self.0.host_str().unwrap_or_default(),
                    path = self.0.path()
                )
            }
        }

        Display(self)
    }
}

/// Represents a client to use for making HTTP requests.
#[derive(Clone)]
pub struct HttpClient {
    /// The underlying HTTP client.
    client: ClientWithMiddleware,
    /// The cache to use for storing previous requests.
    ///
    /// If `None`, the client is not using a cache.
    cache: Option<Arc<Cache<DefaultCacheStorage>>>,
}

impl HttpClient {
    const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(60);
    const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(60);

    /// Constructs a new HTTP client.
    pub fn new() -> Self {
        let client = Client::builder()
            .connect_timeout(Self::DEFAULT_CONNECT_TIMEOUT)
            .read_timeout(Self::DEFAULT_READ_TIMEOUT)
            .build()
            .expect("failed to build HTTP client");

        Self::from_existing(client)
    }

    /// Constructs a new HTTP client using the given cache directory.
    pub fn new_with_cache(cache_dir: impl AsRef<Path>) -> Self {
        let client = Client::builder()
            .connect_timeout(Self::DEFAULT_CONNECT_TIMEOUT)
            .read_timeout(Self::DEFAULT_READ_TIMEOUT)
            .build()
            .expect("failed to build HTTP client");

        Self::from_existing_with_cache(client, cache_dir)
    }

    /// Constructs a new HTTP client using an existing client.
    pub fn from_existing(client: reqwest::Client) -> Self {
        Self {
            client: ClientWithMiddleware::new(client, Vec::new()),
            cache: None,
        }
    }

    /// Constructs a new HTTP client using an existing client and the given
    /// cache directory.
    pub fn from_existing_with_cache(client: reqwest::Client, cache_dir: impl AsRef<Path>) -> Self {
        let cache_dir = cache_dir.as_ref();
        info!(
            "using HTTP download cache directory `{dir}`",
            dir = cache_dir.display()
        );

        let cache = Arc::new(Cache::new(DefaultCacheStorage::new(cache_dir)));

        Self {
            client: ClientBuilder::new(client).with_arc(cache.clone()).build(),
            cache: Some(cache),
        }
    }

    /// Gets the associated cache.
    ///
    /// If `None`, the client is not configured for caching.
    pub fn cache(&self) -> Option<&Cache<DefaultCacheStorage>> {
        self.cache.as_deref()
    }
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for HttpClient {
    type Target = ClientWithMiddleware;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

/// Helper for displaying a message in `Error`.
struct DisplayMessage<'a> {
    /// The status code of the error.
    status: StatusCode,
    /// The message to display.
    ///
    /// If empty, the status code's canonical reason will be used.
    message: &'a str,
}

impl fmt::Display for DisplayMessage<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            write!(
                f,
                " ({reason})",
                reason = self
                    .status
                    .canonical_reason()
                    .unwrap_or("<unknown status code>")
                    .to_lowercase()
            )
        } else {
            write!(f, ": {message}", message = self.message)
        }
    }
}

/// Represents a copy operation error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The operation was canceled.
    #[error("the operation was canceled")]
    Canceled,
    /// Copying between remote locations is not supported.
    #[error("copying between remote locations is not supported")]
    RemoteCopyNotSupported,
    /// A remote URL has an unsupported URL scheme.
    #[error("remote URL has an unsupported URL scheme `{0}`")]
    UnsupportedUrlScheme(String),
    /// Unsupported remote URL.
    #[error("URL `{url}` is not for a supported cloud service", url = .0.display())]
    UnsupportedUrl(Url),
    /// Invalid URl with a `file` scheme.
    #[error("file URL `{url}` cannot be represented as a local path", url = .0.display())]
    InvalidFileUrl(Url),
    /// The specified path is invalid.
    #[error("the specified path cannot be a root directory or empty")]
    InvalidPath,
    /// The remote content was modified during a download.
    #[error("the remote content was modified during the download")]
    RemoteContentModified,
    /// Failed to create a directory.
    #[error("failed to create directory `{path}`: {error}", path = .path.display())]
    DirectoryCreationFailed {
        /// The path to the directory that failed to be created.
        path: PathBuf,
        /// The error that occurred creating the directory.
        error: std::io::Error,
    },
    /// Failed to create a temporary file.
    #[error("failed to create temporary file: {error}")]
    CreateTempFile {
        /// The error that occurred creating the temporary file.
        error: std::io::Error,
    },
    /// Failed to persist a temporary file.
    #[error("failed to persist temporary file: {error}")]
    PersistTempFile {
        /// The error that occurred creating the temporary file.
        error: std::io::Error,
    },
    /// The destination path already exists.
    #[error("the destination path `{path}` already exists", path = .0.display())]
    DestinationExists(PathBuf),
    /// The server returned an error.
    #[error(
        "server returned status {status}{message}",
        status = .status.as_u16(),
        message = DisplayMessage { status: *.status, message }
    )]
    Server {
        /// The response status code.
        status: reqwest::StatusCode,
        /// The response error message.
        ///
        /// This may be the contents of the entire response body.
        message: String,
    },
    /// Server responded with an unexpected `content-range` header.
    #[error(
        "server responded with a `content-range` header that does not start at the requested \
         offset"
    )]
    UnexpectedContentRangeStart,
    /// An Azure error occurred.
    #[error(transparent)]
    Azure(#[from] AzureError),
    /// An S3 error occurred.
    #[error(transparent)]
    S3(#[from] S3Error),
    /// A Google Cloud Storage error occurred.
    #[error(transparent)]
    Google(#[from] GoogleError),
    /// An I/O error occurred.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A directory walking error occurred.
    #[error(transparent)]
    Walk(#[from] walkdir::Error),
    /// A reqwest error occurred.
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    /// A reqwest middleware error occurred.
    #[error(transparent)]
    Middleware(#[from] reqwest_middleware::Error),
    /// A temp file persist error occurred.
    #[error(transparent)]
    Temp(#[from] tempfile::PersistError),
}

impl Error {
    /// Converts the error into a retry error.
    fn into_retry_error(self) -> RetryError<Self> {
        match &self {
            Error::Server { status, .. }
            | Error::Azure(AzureError::UnexpectedResponse { status, .. })
                if status.is_server_error() =>
            {
                RetryError::transient(self)
            }
            Error::Io(_) | Error::Reqwest(_) | Error::Middleware(_) => RetryError::transient(self),
            _ => RetryError::permanent(self),
        }
    }
}

/// Represents a result for copy operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Represents an event that may occur during a file transfer.
#[derive(Debug, Clone)]
pub enum TransferEvent {
    /// A transfer of a file has been started.
    TransferStarted {
        /// The id of the file transfer.
        ///
        /// This is a monotonic counter that is increased every transfer.
        id: u64,
        /// The path of the file being transferred.
        path: PathBuf,
        /// The number of blocks in the file.
        blocks: u64,
        /// The size of the file being transferred.
        ///
        /// This is `None` when downloading a file of unknown size.
        size: Option<u64>,
    },
    /// A transfer of a block has started.
    BlockStarted {
        /// The id of the file transfer.
        id: u64,
        /// The block number being transferred.
        block: u64,
        /// The size of the block being transferred.
        ///
        /// This is `None` when downloading a file of unknown size.
        size: Option<u64>,
    },
    /// A transfer of a block has made progress.
    BlockProgress {
        /// The id of the transfer.
        id: u64,
        /// The block number being transferred.
        block: u64,
        /// The total number of bytes transferred in the block so far.
        transferred: u64,
    },
    /// A transfer of a block has completed.
    BlockCompleted {
        /// The id of the transfer.
        id: u64,
        /// The block number being transferred.
        block: u64,
        /// Whether or not the transfer failed.
        failed: bool,
    },
    /// A file transfer has completed.
    TransferCompleted {
        /// The id of the transfer.
        id: u64,
        /// Whether or not the transfer failed.
        failed: bool,
    },
}

/// Copies a local file to another path.
///
/// This differs from `tokio::fs::copy` in that progress events will be sent.
async fn copy_local(
    source: &Path,
    destination: &Path,
    cancel: CancellationToken,
    events: Option<broadcast::Sender<TransferEvent>>,
) -> Result<()> {
    // The transfer id for the copy.
    const ID: u64 = 0;
    /// The block index for the copy.
    const BLOCK: u64 = 0;

    // Wrap the source stream with a transfer stream to emit events
    let mut reader = StreamReader::new(TransferStream::new(
        ReaderStream::new(BufReader::new(
            OpenOptions::new().read(true).open(source).await?,
        )),
        ID,
        BLOCK,
        0,
        events.clone(),
    ));

    let mut writer = BufWriter::new(
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(destination)
            .await?,
    );

    // Send the transfer and block started event
    if let Some(events) = &events {
        let size = std::fs::metadata(source)?.len();

        events
            .send(TransferEvent::TransferStarted {
                id: ID,
                path: destination.to_path_buf(),
                blocks: 1,
                size: Some(size),
            })
            .ok();

        events
            .send(TransferEvent::BlockStarted {
                id: ID,
                block: BLOCK,
                size: Some(size),
            })
            .ok();
    }

    // Copy the reader stream to the writer stream
    let result = tokio::select! {
        _ = cancel.cancelled() => {
            drop(writer);
            std::fs::remove_file(destination).ok();
            Err(Error::Canceled)
        },
        r = tokio::io::copy(&mut reader, &mut writer) => {
            r?;
            Ok(())
        }
    };

    // Send the block and transfer end event
    if let Some(events) = &events {
        events
            .send(TransferEvent::BlockCompleted {
                id: ID,
                block: BLOCK,
                failed: result.is_err(),
            })
            .ok();

        events
            .send(TransferEvent::TransferCompleted {
                id: ID,
                failed: result.is_err(),
            })
            .ok();
    }

    result
}

/// Copies a source location to a destination location.
///
/// A location may either be a local path or a remote URL.
///
/// If provided, the `events` sender will be used to send transfer events.
///
/// _Note: copying between two remote locations is not supported._
///
/// # Azure Blob Storage
///
/// Supported remote URLs for Azure Blob Storage:
///
/// * `az` schemed URLs in the format `az://<account>/<container>/<blob>`.
/// * `https` schemed URLs in the format `https://<account>.blob.core.windows.net/<container>/<blob>`.
///
/// If authentication is required, the URL is expected to contain a SAS token in
/// its query parameters.
///
/// # Amazon S3
///
/// Supported remote URLs for S3 Storage:
///
/// * `s3` schemed URLs in the format: `s3://<bucket>/<object>` (note: uses the
///   default region).
/// * `https` schemed URLs in the format `https://<bucket>.s3.<region>.amazonaws.com/<object>`.
/// * `https` schemed URLs in the format `https://<region>.s3.amazonaws.com/<bucket>/<object>`.
///
/// If authentication is required, the provided `Config` must have S3
/// authentication information.
///
/// # Google Cloud Storage
///
/// Supported remote URLs for Google Cloud Storage:
///
/// * `gs` schemed URLs in the format: `gs://<bucket>/<object>`.
/// * `https` schemed URLs in the format `https://<bucket>.storage.googleapis.com/<object>`.
/// * `https` schemed URLs in the format `https://storage.googleapis.com/<bucket>/<object>`.
///
/// If authentication is required, the provided `Config` must have Google
/// authentication information.
///
/// Note that [HMAC authentication](https://cloud.google.com/storage/docs/authentication/hmackeys)
/// is used for Google Cloud Storage access.
pub async fn copy(
    config: Config,
    client: HttpClient,
    source: impl Into<Location<'_>>,
    destination: impl Into<Location<'_>>,
    cancel: CancellationToken,
    events: Option<broadcast::Sender<TransferEvent>>,
) -> Result<()> {
    let source = source.into();
    let destination = destination.into();

    match (source, destination) {
        (Location::Path(source), Location::Path(destination)) => {
            if destination.exists() {
                return Err(Error::DestinationExists(destination.to_path_buf()));
            }

            // Two local locations, just perform a copy
            Ok(copy_local(source, destination, cancel, events).await?)
        }
        (Location::Path(source), Location::Url(destination)) => {
            // Perform a copy if the the destination is a local path
            if let Some(destination) = destination.to_local_path()? {
                return copy_local(source, &destination, cancel, events).await;
            }

            if AzureBlobStorageBackend::is_supported_url(&config, &destination) {
                let transfer =
                    FileTransfer::new(AzureBlobStorageBackend::new(config, client, events), cancel);
                transfer.upload(source, destination).await
            } else if S3StorageBackend::is_supported_url(&config, &destination) {
                let transfer =
                    FileTransfer::new(S3StorageBackend::new(config, client, events), cancel);
                transfer.upload(source, destination).await
            } else if GoogleStorageBackend::is_supported_url(&config, &destination) {
                let transfer =
                    FileTransfer::new(GoogleStorageBackend::new(config, client, events), cancel);
                transfer.upload(source, destination).await
            } else {
                Err(Error::UnsupportedUrl(destination))
            }
        }
        (Location::Url(source), Location::Path(destination)) => {
            if destination.exists() {
                return Err(Error::DestinationExists(destination.to_path_buf()));
            }

            // Perform a copy if the the source is a local path
            if let Some(source) = source.to_local_path()? {
                return copy_local(&source, destination, cancel, events).await;
            }

            if AzureBlobStorageBackend::is_supported_url(&config, &source) {
                let transfer =
                    FileTransfer::new(AzureBlobStorageBackend::new(config, client, events), cancel);
                transfer.download(source, destination).await
            } else if S3StorageBackend::is_supported_url(&config, &source) {
                let transfer =
                    FileTransfer::new(S3StorageBackend::new(config, client, events), cancel);
                transfer.download(source, destination).await
            } else if GoogleStorageBackend::is_supported_url(&config, &source) {
                let transfer =
                    FileTransfer::new(GoogleStorageBackend::new(config, client, events), cancel);
                transfer.download(source, destination).await
            } else {
                let transfer =
                    FileTransfer::new(GenericStorageBackend::new(config, client, events), cancel);
                transfer.download(source, destination).await
            }
        }
        (Location::Url(source), Location::Url(destination)) => {
            if let (Some(source), Some(destination)) =
                (source.to_local_path()?, destination.to_local_path()?)
            {
                // Two local locations, just perform a copy
                return copy_local(&source, &destination, cancel, events).await;
            }

            Err(Error::RemoteCopyNotSupported)
        }
    }
}
