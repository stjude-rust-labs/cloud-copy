//! Implementation of file transfers.

use std::io::SeekFrom;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use http_cache_stream_reqwest::CacheStorage;
use http_cache_stream_reqwest::X_CACHE_DIGEST;
use reqwest::StatusCode;
use reqwest::header;
use tempfile::NamedTempFile;
use tokio::fs;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::select;
use tokio_retry2::Retry;
use tokio_util::io::StreamReader;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::info;
use tracing::warn;
use url::Url;
use walkdir::WalkDir;

use crate::Error;
use crate::Location;
use crate::Result;
use crate::TransferEvent;
use crate::UrlExt;
use crate::backend::StorageBackend;
use crate::backend::Upload;
use crate::notify_retry;
use crate::pool::BufferPool;
use crate::streams::TransferStream;

/// Gets the next transfer id.
fn next_id() -> u64 {
    /// The next transfer identifier.
    ///
    /// This is monotonically increasing across all file transfers.
    static NEXT_ID: AtomicU64 = AtomicU64::new(0);

    NEXT_ID.fetch_add(1, Ordering::SeqCst)
}

/// Represents information about a file being uploaded.
#[derive(Clone, Copy)]
struct UploadInfo {
    /// The file transfer id.
    id: u64,
    /// The size of the file being uploaded.
    file_size: u64,
    /// The block size for the upload.
    block_size: u64,
    /// The number of blocks in the file.
    num_blocks: u64,
}

/// Inner state for file transfers.
struct FileTransferInner<B> {
    /// The storage backend to use for transferring files.
    backend: B,
    /// The buffer pool for buffering blocks before writing them to disk.
    pool: BufferPool,
}

impl<B> FileTransferInner<B>
where
    B: StorageBackend + Send + Sync + 'static,
{
    /// Downloads a file to the given destination path.
    async fn download(
        &self,
        source: Url,
        destination: &Path,
        cancel: CancellationToken,
    ) -> Result<()> {
        info!(
            "downloading `{source}` to `{destination}`",
            source = source.display(),
            destination = destination.display(),
        );

        // Start by sending a HEAD request for the file's size and etag
        let response = Retry::spawn_notify(
            self.backend.config().retry_durations(),
            || async {
                select! {
                    biased;
                    _ = cancel.cancelled() => Err(Error::Canceled),
                    r = self.backend.head(source.clone(), true) => r
                }
                .map_err(Error::into_retry_error)
            },
            notify_retry,
        )
        .await?;

        let id = next_id();

        // Check for a strong etag
        let etag = response
            .headers()
            .get(header::ETAG)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| {
                if v.trim_start().starts_with("W/") {
                    None
                } else {
                    Some(v)
                }
            });

        // Get the content length
        let content_length = response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok()?.parse().ok());

        let download_source = source.clone();
        let transfer = async {
            // Start by creating the destination's parent directory
            let parent = destination.parent().ok_or(Error::InvalidPath)?;
            fs::create_dir_all(parent)
                .await
                .map_err(|error| Error::DirectoryCreationFailed {
                    path: parent.to_path_buf(),
                    error,
                })?;

            // Use a temp file that will be atomically renamed when the download completes
            let temp = NamedTempFile::with_prefix_in(".copy", parent)
                .map_err(|error| Error::CreateTempFile { error })?
                .into_temp_path();

            // Download the file with resumable retries
            self.download_with_resume(
                id,
                download_source,
                &temp,
                content_length,
                etag,
                cancel.clone(),
            )
            .await?;

            // Persist the temp file to the destination
            temp.persist_noclobber(destination)
                .map_err(|e| Error::PersistTempFile { error: e.error })?;

            Ok(())
        };

        // Send the transfer started event
        if let Some(events) = self.backend.events() {
            events
                .send(TransferEvent::TransferStarted {
                    id,
                    source: Location::Url(source),
                    destination: Location::Path(destination.to_path_buf()),
                    blocks: 1,
                    size: content_length,
                })
                .ok();
        }

        let result = select! {
            biased;
            _ = cancel.cancelled() => Err(Error::Canceled),
            r = transfer => r,
        };

        // Send the transfer end event
        if let Some(events) = self.backend.events() {
            events
                .send(TransferEvent::TransferCompleted {
                    id,
                    failed: result.is_err(),
                })
                .ok();
        }

        result
    }

    /// Downloads a file with resuming retry.
    async fn download_with_resume(
        &self,
        id: u64,
        source: Url,
        destination: &Path,
        content_length: Option<u64>,
        etag: Option<&str>,
        cancel: CancellationToken,
    ) -> Result<()> {
        let transfer = async {
            let offset = AtomicU64::new(0);

            // Retry the download with resume
            Retry::spawn_notify(
                self.backend.config().retry_durations(),
                || async {
                    let mut current = offset.load(Ordering::SeqCst);

                    let response = if current > 0
                        && let Some(etag) = etag
                    {
                        self.backend
                            .get_at_offset(source.clone(), etag, current)
                            .await?
                    } else {
                        let response = self.backend.get(source.clone()).await?;

                        // Check to see if we should link to the cache location
                        if self.backend.config().link_to_cache()
                            && let Some(digest) = response
                                .headers()
                                .get(X_CACHE_DIGEST)
                                .and_then(|v| v.to_str().ok())
                            && let Some(cache) = self.backend.cache()
                        {
                            let path = cache.storage().body_path(digest);
                            if path.is_file() {
                                // Remove the existing temp file and replace it with a hard link
                                fs::remove_file(destination).await.ok();
                                match fs::hard_link(&path, destination).await {
                                    Ok(_) => {
                                        debug!(
                                            "created a hard link from cache location `{path}`",
                                            path = path.display(),
                                        );
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        warn!(
                                            "failed to create a hard link to cached file \
                                             (performing a copy instead): {e}"
                                        );
                                    }
                                }
                            }
                        }

                        response
                    };

                    // If the response is not partial, start from the beginning
                    if current != 0
                        && (etag.is_none() || response.status() != StatusCode::PARTIAL_CONTENT)
                    {
                        debug!(
                            "resuming download of `{source}` from the beginning of the file",
                            source = source.display()
                        );
                        current = 0;
                    } else if response.status() == StatusCode::PARTIAL_CONTENT {
                        // Ensure the response starts at the requested position
                        if !response
                            .headers()
                            .get(header::CONTENT_RANGE)
                            .and_then(|v| v.to_str().ok())
                            .map(|v| v.trim_start().starts_with(&format!("bytes {current}-")))
                            .unwrap_or(false)
                        {
                            return Err(Error::UnexpectedContentRangeStart.into());
                        }

                        debug!(
                            "resuming download of `{source}` from offset {current}",
                            source = source.display()
                        );
                    }

                    let mut reader = StreamReader::new(TransferStream::new(
                        response.bytes_stream().map_err(std::io::Error::other),
                        id,
                        0,
                        current,
                        self.backend.events().clone(),
                    ));

                    let mut file = fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(false)
                        .open(destination)
                        .await
                        .map_err(|error| Error::CreateTempFile { error })?;

                    file.set_len(current).await.map_err(Error::from)?;
                    file.seek(SeekFrom::Start(current))
                        .await
                        .map_err(Error::from)?;

                    let mut writer = BufWriter::new(file);

                    // Copy the response stream to the temp file
                    // If there is an error, update the current offset so that we resume on next
                    // retry
                    if let Err(e) = tokio::io::copy(&mut reader, &mut writer)
                        .await
                        .map_err(Error::from)
                    {
                        // Flush the writer and determine how much was written
                        writer.flush().await.map_err(Error::from)?;
                        let written = writer
                            .seek(SeekFrom::Current(0))
                            .await
                            .map_err(Error::from)?;
                        offset.store(written, Ordering::SeqCst);
                        return Err(e.into());
                    }

                    Ok(())
                },
                notify_retry,
            )
            .await
        };

        // Send the block started event
        if let Some(events) = self.backend.events() {
            events
                .send(TransferEvent::BlockStarted {
                    id,
                    block: 0,
                    size: content_length,
                })
                .ok();
        }

        let result = select! {
            biased;
            _ = cancel.cancelled() => Err(Error::Canceled),
            r = transfer => r,
        };

        // Send the transfer end event
        if let Some(events) = self.backend.events() {
            events
                .send(TransferEvent::BlockCompleted {
                    id,
                    block: 0,
                    failed: result.is_err(),
                })
                .ok();
        }

        result
    }

    /// Uploads a file with blocks.
    async fn upload(
        self: Arc<Self>,
        upload: Arc<B::Upload>,
        source: &Path,
        info: UploadInfo,
        cancel: CancellationToken,
    ) -> Result<()> {
        let mut parts = vec![Default::default(); info.num_blocks as usize];

        if info.num_blocks == 0 {
            // Spawn a retryable operation to put a single empty block
            let part = Retry::spawn_notify(
                self.backend.config().retry_durations(),
                || async {
                    select! {
                        biased;
                        _ = cancel.cancelled() => Err(Error::Canceled),
                        r = self.upload_block(info.id, 0, Bytes::new(), upload.as_ref(), cancel.clone()) => r,
                    }.map_err(Error::into_retry_error)
                },
                notify_retry,
            )
            .await?;

            if let Some(part) = part {
                parts.push(part);
            }
        } else {
            // Create a stream of tasks for uploading the blocks
            let offset = Arc::new(AtomicU64::new(0));
            let mut stream = stream::iter(0..info.num_blocks)
                .map(|_| {
                    let source = source.to_path_buf();
                    let inner = self.clone();
                    let upload = upload.clone();
                    let offset = offset.clone();
                    let cancel = cancel.clone();

                    tokio::spawn(async move {
                        // Read the block (do not retry if this fails)
                        let block = inner
                            .pool
                            .read_block(&source, info.block_size, info.file_size, &offset)
                            .await?;

                        let block_num = block.num();
                        let bytes = block.into_bytes();

                        // Spawn a retryable operation to put the block
                        Retry::spawn_notify(
                            inner.backend.config().retry_durations(),
                            || async {
                                select! {
                                    biased;
                                    _ = cancel.cancelled() => Err(Error::Canceled),
                                    r = inner.upload_block(info.id, block_num, bytes.clone(), upload.as_ref(), cancel.clone()) => r.map(|p| (block_num, p))
                                }.map_err(Error::into_retry_error)
                            },
                            notify_retry,
                        )
                        .await
                    })
                    .map(|r| r.expect("task panicked"))
                })
                .buffer_unordered(self.backend.config().parallelism());

            // Collect the parts that were uploaded

            loop {
                let result = stream.next().await;
                match result {
                    Some(result) => {
                        let (block, part) = result?;
                        if let Some(part) = part {
                            parts[block as usize] = part;
                        }
                    }
                    None => break,
                }
            }
        }

        // Spawn a retryable operation to finalize the upload
        Retry::spawn_notify(
            self.backend.config().retry_durations(),
            || async {
                select! {
                    biased;
                    _ = cancel.cancelled() => Err(Error::Canceled),
                    r = upload.finalize(&parts) => r,
                }
                .map_err(Error::into_retry_error)
            },
            notify_retry,
        )
        .await
    }

    /// Uploads a block of a file and returns the part that was uploaded.
    async fn upload_block<U: Upload>(
        &self,
        id: u64,
        block: u64,
        bytes: Bytes,
        upload: &U,
        cancel: CancellationToken,
    ) -> Result<Option<U::Part>> {
        if let Some(events) = self.backend.events() {
            events
                .send(TransferEvent::BlockStarted {
                    id,
                    block,
                    size: Some(bytes.len() as u64),
                })
                .ok();
        }

        let result = select! {
            biased;
            _ = cancel.cancelled() => Err(Error::Canceled),
            r = upload.put(id, block, bytes) => r,
        };

        if let Some(events) = self.backend.events() {
            events
                .send(TransferEvent::BlockCompleted {
                    id,
                    block,
                    failed: result.is_err(),
                })
                .ok();
        }

        result
    }
}

/// Represents a file transfer.
#[derive(Clone)]
pub struct FileTransfer<B> {
    /// The inner file transfer.
    inner: Arc<FileTransferInner<B>>,
    /// The cancellation token for canceling the transfer.
    cancel: CancellationToken,
}

impl<B> FileTransfer<B>
where
    B: StorageBackend + Send + Sync + 'static,
{
    /// Constructs a new file transfer with the given storage backend.
    pub fn new(backend: B, cancel: CancellationToken) -> Self {
        let pool = BufferPool::new(backend.config().parallelism());
        Self {
            inner: Arc::new(FileTransferInner { backend, pool }),
            cancel,
        }
    }

    /// Downloads from the given source URL to the given destination path.
    ///
    /// If the source URL is a "directory", the files in the directory will be
    /// downloaded relative to the destination path.
    pub async fn download(&self, source: Url, destination: impl AsRef<Path>) -> Result<()> {
        let destination = destination.as_ref();

        // Start by walking the given URL for files to download
        let paths = Retry::spawn_notify(
            self.inner.backend.config().retry_durations(),
            || async {
                select! {
                    biased;
                    _ = self.cancel.cancelled() => Err(Error::Canceled),
                    r = self.inner.backend.walk(source.clone()) => r
                }
                .map_err(Error::into_retry_error)
            },
            notify_retry,
        )
        .await?;

        // Delete the destination if it exists
        if let Ok(metadata) = destination.metadata() {
            if metadata.is_file() {
                fs::remove_file(destination).await?;
            } else if metadata.is_dir() {
                fs::remove_dir_all(destination).await?;
            }
        }

        // If there are no files relative to the given URL, download the URL a single
        // file
        if paths.is_empty() {
            return self
                .inner
                .download(source, destination, self.cancel.clone())
                .await;
        }

        // Otherwise, download each file in turn
        let mut downloads = stream::iter(paths)
            .map(move |path| {
                let mut source = source.clone();
                let mut destination = destination.to_path_buf();

                // Adjust source and destination based on the provided relative URL path
                {
                    let mut segments = source.path_segments_mut().expect("URL should have a path");
                    for segment in path.split('/') {
                        segments.push(segment);
                        destination.push(segment);
                    }
                }

                let inner = self.inner.clone();
                let cancel = self.cancel.clone();
                tokio::spawn(async move { inner.download(source, &destination, cancel).await })
                    .map(|r| r.expect("task panicked"))
            })
            .buffer_unordered(self.inner.backend.config().parallelism());

        loop {
            let result = downloads.next().await;
            match result {
                Some(r) => r?,
                None => break,
            }
        }

        Ok(())
    }

    /// Uploads the given source path to the given destination URL.
    ///
    /// If the path is a directory, each file the directory recursively contains
    /// will be uploaded.
    pub async fn upload(&self, source: impl AsRef<Path>, destination: Url) -> Result<()> {
        let source = source.as_ref();

        // Recursively walk the path looking for files to upload
        for entry in WalkDir::new(source) {
            let entry = entry?;
            let metadata = entry.metadata()?;

            // We're recursively walking the directory; ignore directory entries
            if metadata.is_dir() {
                continue;
            }

            // Calculate the relative path for the file
            let relative_path = entry
                .path()
                .strip_prefix(source)
                .expect("failed to strip path prefix");

            let destination = self.inner.backend.join_url(
                destination.clone(),
                relative_path
                    .components()
                    .map(|c| c.as_os_str().to_str().expect("path not UTF-8")),
            )?;

            // Perform the upload
            let result = self
                .upload_file(entry.path(), destination, metadata.len())
                .await;

            // Send the transfer completed event
            result?;
        }

        Ok(())
    }

    /// Uploads the given file to the given destination.
    async fn upload_file(&self, source: &Path, destination: Url, file_size: u64) -> Result<()> {
        // Calculate the content digest
        let digest = self
            .inner
            .backend
            .config()
            .hash_algorithm()
            .calculate_content_digest(source)
            .await?;

        // Create the upload (retryable)
        // This is performed before we send transfer events in case the resource already
        // exists and we're not overwriting
        let upload = Arc::new(
            Retry::spawn_notify(
                self.inner.backend.config().retry_durations(),
                || async {
                    select! {
                        biased;
                        _ = self.cancel.cancelled() => Err(Error::Canceled),
                        r =  self.inner.backend.new_upload(destination.clone(), digest.clone()) => r,
                    }
                    .map_err(Error::into_retry_error)
                },
                notify_retry,
            )
            .await?,
        );

        info!(
            "uploading `{source}` to `{destination}`",
            source = source.display(),
            destination = destination.display(),
        );

        // Calculate the block size and the number of blocks to upload
        let block_size = self.inner.backend.block_size(file_size)?;
        let num_blocks = if file_size == 0 {
            0
        } else {
            file_size.div_ceil(block_size)
        };

        let id = next_id();

        debug!(
            "file `{source}` is {file_size} bytes and will be uploaded with {num_blocks} block(s) \
             of size {block_size}",
            source = source.display()
        );

        if let Some(events) = self.inner.backend.events() {
            events
                .send(TransferEvent::TransferStarted {
                    id,
                    source: Location::Path(source.to_path_buf()),
                    destination: Location::Url(destination),
                    blocks: num_blocks,
                    size: Some(file_size),
                })
                .ok();
        }

        let info = UploadInfo {
            id,
            file_size,
            block_size,
            num_blocks,
        };

        let result = self
            .inner
            .clone()
            .upload(upload, source, info, self.cancel.clone())
            .await;

        if let Some(events) = self.inner.backend.events() {
            events
                .send(TransferEvent::TransferCompleted {
                    id,
                    failed: result.is_err(),
                })
                .ok();
        }

        result
    }
}
