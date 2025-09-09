//! Implementation of a generic storage backend.
//!
//! The generic storage backend can only be used for downloading files.

use std::borrow::Cow;

use bytes::Bytes;
use chrono::Utc;
use http_cache_stream_reqwest::Cache;
use http_cache_stream_reqwest::storage::DefaultCacheStorage;
use reqwest::Response;
use reqwest::StatusCode;
use reqwest::header;
use tokio::sync::broadcast;
use tracing::debug;
use url::Url;

use crate::Config;
use crate::Error;
use crate::HttpClient;
use crate::Result;
use crate::TransferEvent;
use crate::USER_AGENT;
use crate::UrlExt;
use crate::backend::StorageBackend;
use crate::backend::Upload;

/// Helper trait for converting responses into `Error`.
trait IntoError {
    /// Converts a generic error response to a `Error`.
    async fn into_error(self) -> Error;
}

impl IntoError for Response {
    async fn into_error(self) -> Error {
        let status = self.status();
        let text: String = match self.text().await {
            Ok(text) => text,
            Err(e) => return e.into(),
        };

        Error::Server {
            status,
            message: text,
        }
    }
}

/// Represents a generic upload.
///
/// As the generic backend cannot be used to upload files, this implementation
/// panics on use.
pub struct GenericUpload;

impl Upload for GenericUpload {
    type Part = ();

    async fn put(&self, _: u64, _: u64, _: Bytes) -> Result<Option<Self::Part>> {
        unimplemented!()
    }

    async fn finalize(&self, _: &[Self::Part]) -> Result<()> {
        unimplemented!()
    }
}

/// Represents a generic storage backend.
///
/// The generic storage backend can only be used to download files.
pub struct GenericStorageBackend {
    /// The configuration to use for transferring files.
    config: Config,
    /// The HTTP client to use for transferring files.
    client: HttpClient,
    /// The channel for sending transfer events.
    events: Option<broadcast::Sender<TransferEvent>>,
}

impl GenericStorageBackend {
    /// Constructs a new generic storage backend with the given configuration
    /// and events channel.
    pub fn new(
        config: Config,
        client: HttpClient,
        events: Option<broadcast::Sender<TransferEvent>>,
    ) -> Self {
        Self {
            config,
            client,
            events,
        }
    }
}

impl StorageBackend for GenericStorageBackend {
    type Upload = GenericUpload;

    fn config(&self) -> &Config {
        &self.config
    }

    fn cache(&self) -> Option<&Cache<DefaultCacheStorage>> {
        self.client.cache()
    }

    fn events(&self) -> &Option<broadcast::Sender<TransferEvent>> {
        &self.events
    }

    fn block_size(&self, _: u64) -> Result<u64> {
        // Return the block size if one was specified
        if let Some(size) = self.config.block_size {
            return Ok(size);
        }

        // Used a fixed block size of 4 MiB
        Ok(4 * 1024 * 1024)
    }

    fn is_supported_url(_: &Config, _: &Url) -> bool {
        true
    }

    fn rewrite_url<'a>(_: &Config, url: &'a Url) -> Result<Cow<'a, Url>> {
        Ok(Cow::Borrowed(url))
    }

    fn join_url<'a>(&self, mut url: Url, segments: impl Iterator<Item = &'a str>) -> Result<Url> {
        // Append on the segments
        {
            let mut existing = url.path_segments_mut().expect("url should have path");
            existing.pop_if_empty();
            existing.extend(segments);
        }

        Ok(url)
    }

    async fn head(&self, url: Url) -> Result<Response> {
        debug!("sending HEAD request for `{url}`", url = url.display());

        let response = self
            .client
            .head(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::DATE, Utc::now().to_rfc2822())
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(response.into_error().await);
        }

        Ok(response)
    }

    async fn get(&self, url: Url) -> Result<Response> {
        debug!("sending GET request for `{url}`", url = url.display());

        let response = self
            .client
            .get(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::DATE, Utc::now().to_rfc2822())
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(response.into_error().await);
        }

        Ok(response)
    }

    async fn get_at_offset(&self, url: Url, etag: &str, offset: u64) -> Result<Response> {
        debug!(
            "sending GET request at offset {offset} for `{url}`",
            url = url.display(),
        );

        let response = self
            .client
            .get(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::DATE, Utc::now().to_rfc2822())
            .header(header::RANGE, format!("bytes={offset}-"))
            .header(header::IF_MATCH, etag)
            .send()
            .await?;

        let status = response.status();

        // Handle precondition failed as remote content modified
        if status == StatusCode::PRECONDITION_FAILED {
            return Err(Error::RemoteContentModified);
        }

        // Handle error response
        if !status.is_success() {
            return Err(response.into_error().await);
        }

        Ok(response)
    }

    async fn walk(&self, _: Url) -> Result<Vec<String>> {
        // The generic backend treats all URLs as files.
        Ok(Vec::default())
    }

    async fn exists(&self, url: Url) -> Result<bool> {
        debug!("checking existence of `{url}`", url = url.display());

        let response = self
            .client
            .head(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::DATE, Utc::now().to_rfc2822())
            .send()
            .await?;

        if !response.status().is_success() {
            if response.status() == StatusCode::NOT_FOUND {
                return Ok(false);
            }

            return Err(response.into_error().await);
        }

        Ok(true)
    }

    async fn new_upload(&self, _: Url) -> Result<Self::Upload> {
        panic!("generic storage backend cannot be used for uploading");
    }
}
