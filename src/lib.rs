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
// This is disabled until upstream crates are fixed for nightly
//#![cfg_attr(docsrs, feature(doc_cfg))]

use std::borrow::Cow;
use std::fmt;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use chrono::DateTime;
use chrono::Datelike;
use chrono::Timelike;
use chrono::Utc;
use http_cache_stream_reqwest::Cache;
use http_cache_stream_reqwest::storage::DefaultCacheStorage;
use reqwest::Client;
use reqwest::StatusCode;
use reqwest::header;
use reqwest_middleware::ClientBuilder;
use reqwest_middleware::ClientWithMiddleware;
use sha2::Digest;
use sha2::Sha256;
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
use crate::backend::azure::AZURE_CONTENT_DIGEST_HEADER;
use crate::backend::azure::AzureBlobStorageBackend;
use crate::backend::generic::GenericStorageBackend;
use crate::backend::google::GOOGLE_CONTENT_DIGEST_HEADER;
use crate::backend::google::GoogleStorageBackend;
use crate::backend::s3::AWS_CONTENT_DIGEST_HEADER;
use crate::backend::s3::S3StorageBackend;
use crate::streams::TransferStream;
use crate::transfer::FileTransfer;

mod backend;
#[cfg(feature = "cli")]
#[cfg_attr(docsrs, doc(cfg(feature = "cli")))]
pub mod cli;
mod config;
mod generator;
mod pool;
mod streams;
mod transfer;

pub use backend::azure::AzureError;
pub use backend::google::GoogleError;
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

/// Creates a SHA-256 digest of the given bytes encoded as a hex string.
fn sha256_hex_string(bytes: impl AsRef<[u8]>) -> String {
    let mut hash = Sha256::new();
    hash.update(bytes);
    hex::encode(hash.finalize())
}

trait DateTimeExt {
    /// Converts a [`DateTime`] to a HTTP date string.
    ///
    /// See: <https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Date>
    fn to_http_date(&self) -> String;
}

impl DateTimeExt for DateTime<Utc> {
    fn to_http_date(&self) -> String {
        let mon = match self.month() {
            1 => "Jan",
            2 => "Feb",
            3 => "Mar",
            4 => "Apr",
            5 => "May",
            6 => "Jun",
            7 => "Jul",
            8 => "Aug",
            9 => "Sep",
            10 => "Oct",
            11 => "Nov",
            12 => "Dec",
            _ => unreachable!(),
        };

        format!(
            "{weekday}, {day:02} {mon} {year:04} {hour:02}:{minute:02}:{second:02} GMT",
            weekday = self.weekday(),
            day = self.day(),
            year = self.year(),
            hour = self.hour(),
            minute = self.minute(),
            second = self.second()
        )
    }
}

/// Represents either a local or remote location.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Location {
    /// The location is a local path.
    Path(PathBuf),
    /// The location is a URL.
    Url(Url),
}

impl Location {
    /// Constructs a new location from a string.
    pub fn new(s: &str) -> Self {
        match s.parse::<Url>() {
            Ok(url) => Self::Url(url),
            Err(_) => Self::Path(s.into()),
        }
    }
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Path(path) => write!(f, "{path}", path = path.display()),
            Self::Url(url) => write!(f, "{url}", url = url.display()),
        }
    }
}

impl From<&str> for Location {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<&String> for Location {
    fn from(value: &String) -> Self {
        Self::new(value)
    }
}

impl From<&Path> for Location {
    fn from(value: &Path) -> Self {
        Self::Path(value.to_path_buf())
    }
}

impl From<&PathBuf> for Location {
    fn from(value: &PathBuf) -> Self {
        Self::Path(value.clone())
    }
}

impl From<PathBuf> for Location {
    fn from(value: PathBuf) -> Self {
        Self::Path(value)
    }
}

impl From<&Url> for Location {
    fn from(value: &Url) -> Self {
        Self::Url(value.clone())
    }
}

impl From<Url> for Location {
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
    /// This is used to prevent potentially sensitive information from being
    /// displayed to users.
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
    pub fn new_with_cache(config: Config, cache_dir: impl AsRef<Path>) -> Self {
        let client = Client::builder()
            .connect_timeout(Self::DEFAULT_CONNECT_TIMEOUT)
            .read_timeout(Self::DEFAULT_READ_TIMEOUT)
            .build()
            .expect("failed to build HTTP client");

        Self::from_existing_with_cache(config, client, cache_dir)
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
    pub fn from_existing_with_cache(
        config: Config,
        client: reqwest::Client,
        cache_dir: impl AsRef<Path>,
    ) -> Self {
        let cache_dir = cache_dir.as_ref();
        info!(
            "using HTTP download cache directory `{dir}`",
            dir = cache_dir.display()
        );

        // We must use a revalidation hook to support Azure Storage, which uses
        // `If-None-Match` as part of its authentication signature
        let cache = Arc::new(
            Cache::new(DefaultCacheStorage::new(cache_dir)).with_revalidation_hook(
                move |request, headers| {
                    backend::azure::revalidation_hook(&config, request, headers)?;
                    Ok(())
                },
            ),
        );

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
    /// The local destination path already exists.
    #[error("the destination path `{path}` already exists", path = .0.display())]
    LocalDestinationExists(PathBuf),
    /// The remote destination URL already exists.
    #[error("the destination URL `{url}` already exists", url = .0.display())]
    RemoteDestinationExists(Url),
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
    /// Server responded with an invalid `Content-Digest` header.
    #[error("invalid content digest `{0}`")]
    InvalidContentDigest(String),
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
        /// The location of the source.
        source: Location,
        /// The location of the destination.
        destination: Location,
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
                source: Location::Path(source.to_path_buf()),
                destination: Location::Path(destination.to_path_buf()),
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
/// If authentication is required, the provided `Config` must have Azure
/// authentication information.
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
    source: impl Into<Location>,
    destination: impl Into<Location>,
    cancel: CancellationToken,
    events: Option<broadcast::Sender<TransferEvent>>,
) -> Result<()> {
    let source = source.into();
    let destination = destination.into();

    match (source, destination) {
        (Location::Path(source), Location::Path(destination)) => {
            if !config.overwrite() && destination.exists() {
                return Err(Error::LocalDestinationExists(destination.to_path_buf()));
            }

            // Two local locations, just perform a copy
            Ok(copy_local(&source, &destination, cancel, events).await?)
        }
        (Location::Path(source), Location::Url(destination)) => {
            // Perform a copy if the the destination is a local path
            if let Some(destination) = destination.to_local_path()? {
                return copy_local(&source, &destination, cancel, events).await;
            }

            if AzureBlobStorageBackend::is_supported_url(&config, &destination) {
                let destination = AzureBlobStorageBackend::rewrite_url(&config, &destination)?;
                let transfer =
                    FileTransfer::new(AzureBlobStorageBackend::new(config, client, events), cancel);
                transfer.upload(source, destination.into_owned()).await
            } else if S3StorageBackend::is_supported_url(&config, &destination) {
                let destination = S3StorageBackend::rewrite_url(&config, &destination)?;
                let transfer =
                    FileTransfer::new(S3StorageBackend::new(config, client, events), cancel);
                transfer.upload(source, destination.into_owned()).await
            } else if GoogleStorageBackend::is_supported_url(&config, &destination) {
                let destination = GoogleStorageBackend::rewrite_url(&config, &destination)?;
                let transfer =
                    FileTransfer::new(GoogleStorageBackend::new(config, client, events), cancel);
                transfer.upload(source, destination.into_owned()).await
            } else {
                Err(Error::UnsupportedUrl(destination))
            }
        }
        (Location::Url(source), Location::Path(destination)) => {
            if !config.overwrite() && destination.exists() {
                return Err(Error::LocalDestinationExists(destination.to_path_buf()));
            }

            // Perform a copy if the the source is a local path
            if let Some(source) = source.to_local_path()? {
                return copy_local(&source, &destination, cancel, events).await;
            }

            if AzureBlobStorageBackend::is_supported_url(&config, &source) {
                let source = AzureBlobStorageBackend::rewrite_url(&config, &source)?;
                let transfer =
                    FileTransfer::new(AzureBlobStorageBackend::new(config, client, events), cancel);
                transfer.download(source.into_owned(), destination).await
            } else if S3StorageBackend::is_supported_url(&config, &source) {
                let source = S3StorageBackend::rewrite_url(&config, &source)?;
                let transfer =
                    FileTransfer::new(S3StorageBackend::new(config, client, events), cancel);
                transfer.download(source.into_owned(), destination).await
            } else if GoogleStorageBackend::is_supported_url(&config, &source) {
                let source = GoogleStorageBackend::rewrite_url(&config, &source)?;
                let transfer =
                    FileTransfer::new(GoogleStorageBackend::new(config, client, events), cancel);
                transfer.download(source.into_owned(), destination).await
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

/// Re-writes a cloud storage schemed URL (e.g. `az://`, `s3://`, `gs://`) to a corresponding `https://` URL.
///
/// If the URL is not a cloud storage schemed URL, the given URL is returned.
///
/// Returns an error if the given URL is not a valid cloud storage URL.
pub fn rewrite_url<'a>(config: &Config, url: &'a Url) -> Result<Cow<'a, Url>> {
    if AzureBlobStorageBackend::is_supported_url(config, url) {
        AzureBlobStorageBackend::rewrite_url(config, url)
    } else if S3StorageBackend::is_supported_url(config, url) {
        S3StorageBackend::rewrite_url(config, url)
    } else if GoogleStorageBackend::is_supported_url(config, url) {
        GoogleStorageBackend::rewrite_url(config, url)
    } else {
        Ok(Cow::Borrowed(url))
    }
}

/// Walks a given storage URL as if it were a directory.
///
/// Returns a list of relative paths from the given URL.
///
/// If the given storage URL is not a directory, an empty list is returned.
pub async fn walk(config: Config, client: HttpClient, mut url: Url) -> Result<Vec<String>> {
    if let Ok(mut segments) = url.path_segments_mut() {
        // Push an empty segment to treat the URL as a directory
        // This ensures there is no leading slash in the returned relative paths.
        segments.pop_if_empty().push("");
    }

    if AzureBlobStorageBackend::is_supported_url(&config, &url) {
        let url = AzureBlobStorageBackend::rewrite_url(&config, &url)?;
        AzureBlobStorageBackend::new(config, client, None)
            .walk(url.into_owned())
            .await
    } else if S3StorageBackend::is_supported_url(&config, &url) {
        let url = S3StorageBackend::rewrite_url(&config, &url)?;
        S3StorageBackend::new(config, client, None)
            .walk(url.into_owned())
            .await
    } else if GoogleStorageBackend::is_supported_url(&config, &url) {
        let url = GoogleStorageBackend::rewrite_url(&config, &url)?;
        GoogleStorageBackend::new(config, client, None)
            .walk(url.into_owned())
            .await
    } else {
        Err(Error::UnsupportedUrl(url))
    }
}

/// Represents the content digest of a resource.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ContentDigest {
    /// The digest was produced from a hash algorithm specified via a
    /// [`Content-Digest`][content-digest] header.
    ///
    /// [content-digest]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Digest
    Hash {
        /// The hash algorithm that produced the digest.
        algorithm: String,
        /// The bytes of the digest.
        digest: Vec<u8>,
    },
    /// The digest is from a `ETag` header.
    ///
    /// The `ETag` is always a strong validator.
    ETag(String),
}

impl ContentDigest {
    /// Parses a [`Content-Digest`][content-digest] header value.
    ///
    /// Returns `None` if the header value is not valid.
    ///
    /// [content-digest]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Digest
    pub fn parse_header(value: &str) -> Result<Self> {
        fn parse(value: &str) -> Option<(String, Vec<u8>)> {
            let (algorithm, digest) = value.split_once('=')?;
            let digest = digest.strip_prefix(':')?.strip_suffix(':')?;
            Some((algorithm.to_string(), BASE64_STANDARD.decode(digest).ok()?))
        }

        let (algorithm, digest) =
            parse(value).ok_or_else(|| Error::InvalidContentDigest(value.to_string()))?;
        Ok(Self::Hash { algorithm, digest })
    }
}

/// Retrieves the known content digest of the given URL.
///
/// A `HEAD` request is made for the URL and the response headers are checked:
///
/// * If a [`Content-Digest`][content-digest] header is present, the value is
///   returned as [`ContentDigest::Hash`].
/// * If the resource is a supported cloud storage object and the object has a
///   content digest metadata header, the value is returned as
///   [`ContentDigest::Hash`].
/// * If the response contains a strong `ETag` header, the value is returned as
///   [`ContentDigest::ETag`].
///
/// Returns `Ok(None)` If the resource does not have a known content digest.
///
/// [content-digest]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Digest
pub async fn get_content_digest(
    config: Config,
    client: HttpClient,
    url: Url,
) -> Result<Option<ContentDigest>> {
    // Issue the `HEAD` request
    let (response, metadata) = if AzureBlobStorageBackend::is_supported_url(&config, &url) {
        let url = AzureBlobStorageBackend::rewrite_url(&config, &url)?;
        (
            AzureBlobStorageBackend::new(config, client, None)
                .head(url.into_owned(), true)
                .await?,
            Some(AZURE_CONTENT_DIGEST_HEADER),
        )
    } else if S3StorageBackend::is_supported_url(&config, &url) {
        let url = S3StorageBackend::rewrite_url(&config, &url)?;
        (
            S3StorageBackend::new(config, client, None)
                .head(url.into_owned(), true)
                .await?,
            Some(AWS_CONTENT_DIGEST_HEADER),
        )
    } else if GoogleStorageBackend::is_supported_url(&config, &url) {
        let url = GoogleStorageBackend::rewrite_url(&config, &url)?;
        (
            GoogleStorageBackend::new(config, client, None)
                .head(url.into_owned(), true)
                .await?,
            Some(GOOGLE_CONTENT_DIGEST_HEADER),
        )
    } else {
        (
            GenericStorageBackend::new(config, client, None)
                .head(url, true)
                .await?,
            None,
        )
    };

    // Check for `Content-Digest` header first
    let headers = response.headers();
    if let Some(value) = headers.get("content-digest").and_then(|v| v.to_str().ok()) {
        return Ok(Some(ContentDigest::parse_header(value)?));
    }

    // Check for the backend metadata header next
    if let Some(metadata) = metadata
        && let Some(value) = headers.get(metadata).and_then(|v| v.to_str().ok())
    {
        return Ok(Some(ContentDigest::parse_header(value)?));
    }

    // Finally, check for a strong `ETag` header
    if let Some(value) = headers.get(header::ETAG).and_then(|v| v.to_str().ok())
        && !value.starts_with("W/")
    {
        return Ok(Some(ContentDigest::ETag(value.to_string())));
    }

    Ok(None)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn rewrite_urls() {
        let config = Config::default();

        assert_eq!(
            rewrite_url(&config, &"http://example.com".parse().unwrap())
                .unwrap()
                .as_str(),
            "http://example.com/"
        );

        assert_eq!(
            rewrite_url(&config, &"az://foo/bar/baz".parse().unwrap())
                .unwrap()
                .as_str(),
            "https://foo.blob.core.windows.net/bar/baz"
        );

        assert_eq!(
            rewrite_url(&config, &"s3://foo/bar/baz".parse().unwrap())
                .unwrap()
                .as_str(),
            "https://foo.s3.us-east-1.amazonaws.com/bar/baz"
        );

        assert_eq!(
            rewrite_url(&config, &"gs://foo/bar/baz".parse().unwrap())
                .unwrap()
                .as_str(),
            "https://foo.storage.googleapis.com/bar/baz"
        );

        let config = Config::builder()
            .with_s3(S3Config::default().with_region("my-region"))
            .build();

        assert_eq!(
            rewrite_url(&config, &"s3://foo/bar/baz".parse().unwrap())
                .unwrap()
                .as_str(),
            "https://foo.s3.my-region.amazonaws.com/bar/baz"
        );

        let config = Config::builder()
            .with_azure(AzureConfig::default().with_use_azurite(true))
            .with_s3(S3Config::default().with_use_localstack(true))
            .build();

        assert_eq!(
            rewrite_url(&config, &"az://foo/bar/baz".parse().unwrap())
                .unwrap()
                .as_str(),
            "http://foo.blob.core.windows.net.localhost:10000/bar/baz"
        );

        assert_eq!(
            rewrite_url(&config, &"s3://foo/bar/baz".parse().unwrap())
                .unwrap()
                .as_str(),
            "http://foo.s3.us-east-1.localhost.localstack.cloud:4566/bar/baz"
        );
    }
}
