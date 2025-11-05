//! Implementation of storage backends.

use std::borrow::Cow;

use bytes::Bytes;
use http_cache_stream_reqwest::Cache;
use http_cache_stream_reqwest::storage::DefaultCacheStorage;
use reqwest::Response;
use tokio::sync::broadcast;
use url::Url;

use crate::Config;
use crate::Result;
use crate::TransferEvent;

pub(crate) mod auth;
pub(crate) mod azure;
pub(crate) mod generic;
pub(crate) mod google;
pub(crate) mod s3;

/// Represents an abstract file upload.
pub trait Upload: Send + Sync + 'static {
    /// Represents information about a part that was uploaded.
    type Part: Default + Clone + Send;

    /// Puts a block as part of the upload.
    ///
    /// Upon success, returns information about the part that was uploaded.
    ///
    /// Returns `Ok(None)` if the put wasn't necessary (i.e. an empty block for
    /// some backends).
    fn put(
        &self,
        id: u64,
        block: u64,
        bytes: Bytes,
    ) -> impl Future<Output = Result<Option<Self::Part>>> + Send;

    /// Finalizes the upload given the parts that were uploaded.
    fn finalize(&self, parts: &[Self::Part]) -> impl Future<Output = Result<()>> + Send;
}

/// Represents an abstraction of a storage backend.
pub trait StorageBackend {
    /// The upload type the backend uses.
    type Upload: Upload;

    /// Gets the configuration used by the backend.
    fn config(&self) -> &Config;

    /// Gets the HTTP cache used by the backend.
    ///
    /// Returns `None` if caching is not enabled.
    fn cache(&self) -> Option<&Cache<DefaultCacheStorage>>;

    /// Gets the channel for sending transfer events.
    fn events(&self) -> &Option<broadcast::Sender<TransferEvent>>;

    /// Gets the block size given the size of a file.
    fn block_size(&self, file_size: u64) -> Result<u64>;

    /// Whether or not the URL is supported by this backend.
    fn is_supported_url(config: &Config, url: &Url) -> bool;

    /// Rewrites the given URL.
    ///
    /// If the URL is using a cloud-specific scheme, the URL is rewritten to a
    /// `https` schemed URL.
    ///
    /// Otherwise, the given URL is returned as-is.
    fn rewrite_url<'a>(config: &Config, url: &'a Url) -> Result<Cow<'a, Url>>;

    /// Joins segments to a URL to form a new URL.
    fn join_url<'a>(&self, url: Url, segments: impl Iterator<Item = &'a str>) -> Result<Url>;

    /// Sends a HEAD request for the given URL.
    ///
    /// If `must_exist` is `true`, an error should be returned on a 404
    /// response.
    ///
    /// Returns an error if the request was not successful.
    fn head(&self, url: Url, must_exist: bool) -> impl Future<Output = Result<Response>> + Send;

    /// Sends a GET request for the given URL.
    ///
    /// Returns an error if the request was not successful.
    fn get(&self, url: Url) -> impl Future<Output = Result<Response>> + Send;

    /// Sends a conditional ranged GET request for the given URL at the given
    /// start offset.
    ///
    /// Returns `Ok(_)` if the response returns a 200 (full content) or 206
    /// (partial content).
    ///
    /// Returns an error if the request was not successful or if the etag did
    /// not match (condition not met).
    fn get_at_offset(
        &self,
        url: Url,
        etag: &str,
        offset: u64,
    ) -> impl Future<Output = Result<Response>> + Send;

    /// Walks a given storage URL as if it were a directory.
    ///
    /// Returns a list of relative paths from the given URL.
    ///
    /// If the given storage URL is not a directory, an empty list is returned.
    fn walk(&self, url: Url) -> impl Future<Output = Result<Vec<String>>> + Send;

    /// Creates a new upload.
    ///
    /// If `digest` is `Some`, it is expected to be a `Content-Digest` header
    /// value.
    ///
    /// See: <https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Digest>
    fn new_upload(
        &self,
        digest: Option<String>,
        url: Url,
    ) -> impl Future<Output = Result<Self::Upload>> + Send;
}
