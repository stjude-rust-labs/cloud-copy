//! Implementation of file transfers.

use std::fs::File;
use std::io::SeekFrom;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryFutureExt;
use futures::TryStreamExt;
use futures::stream;
use http_cache_stream_reqwest::CacheStorage;
use http_cache_stream_reqwest::X_CACHE_DIGEST;
use reqwest::StatusCode;
use reqwest::header;
use tempfile::NamedTempFile;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::select;
use tokio::task::spawn_blocking;
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
use crate::pool::BufferGuard;
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

/// Represents information about a file download.
struct DownloadInfo<'a> {
    /// The file transfer id.
    id: u64,
    /// The URL of the file being downloaded.
    source: Url,
    /// The destination path of the file.
    destination: &'a Path,
}

/// Represents information about a block being downloaded.
struct BlockDownloadInfo {
    /// The file transfer id.
    id: u64,
    /// The ETAG of the resource being downloaded.
    etag: Arc<String>,
    /// The file being written.
    file: Arc<File>,
    /// The block number being downloaded.
    block: u64,
    /// The buffer to use to read and write the block.
    buffer: BufferGuard,
    /// The range in the file being downloaded.
    range: Range<u64>,
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
        self: &Arc<Self>,
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

        // Check to see if ranged requests are accepted
        let accept_ranges = response
            .headers()
            .get(header::ACCEPT_RANGES)
            .map(|v| v.to_str().ok() == Some("bytes"))
            .unwrap_or(false);

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

            // Check to see if we can transfer the file in blocks; this requires a known
            // file size, a strong etag, the server to accept ranged requests, and that
            // we're not using a cache as our cache implementation does not support ranged
            // requests.
            match (accept_ranges, content_length, etag) {
                (true, Some(content_length), Some(etag))
                    if self.backend.cache().is_none() && !etag.starts_with("W/") =>
                {
                    // Download the file in blocks
                    self.download_in_blocks(
                        DownloadInfo {
                            id,
                            source: download_source,
                            destination: &temp,
                        },
                        content_length,
                        etag,
                        cancel.clone(),
                    )
                    .await?;
                }
                _ => {
                    // Download the file with resumable retries
                    self.download_with_resume(
                        DownloadInfo {
                            id,
                            source: download_source,
                            destination: &temp,
                        },
                        content_length,
                        etag,
                        accept_ranges,
                        cancel.clone(),
                    )
                    .await?;
                }
            }

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
    ///
    /// If the server supports ranged requests, a retry operation will resume
    /// where it left off.
    ///
    /// If the server does not support ranged requests, a retry operation will
    /// restart from the beginning.
    async fn download_with_resume(
        &self,
        info: DownloadInfo<'_>,
        content_length: Option<u64>,
        etag: Option<&str>,
        accept_ranges: bool,
        cancel: CancellationToken,
    ) -> Result<()> {
        if accept_ranges {
            debug!(
                "file `{source}` will be downloaded with resumable retries",
                source = info.source.display()
            );
        } else {
            debug!(
                "file `{source}` will be downloaded with resumable retries",
                source = info.source.display()
            );
        }

        let transfer = async {
            let offset = AtomicU64::new(0);

            // Retry the download with resume (if server accepts ranged requests)
            Retry::spawn_notify(
                self.backend.config().retry_durations(),
                || async {
                    let mut current = offset.load(Ordering::SeqCst);

                    let response = if accept_ranges
                        && current > 0
                        && let Some(etag) = etag
                    {
                        self.backend
                            .get_range(info.source.clone(), etag, current, None)
                            .await?
                    } else {
                        let response = self.backend.get(info.source.clone()).await?;

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
                                fs::remove_file(info.destination).await.ok();
                                match fs::hard_link(&path, info.destination).await {
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
                    if response.status() != StatusCode::PARTIAL_CONTENT {
                        if current > 0 {
                            debug!(
                                "resuming download of `{source}` from the beginning",
                                source = info.source.display()
                            );
                        }

                        current = 0;
                    } else {
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
                            source = info.source.display()
                        );
                    }

                    let mut reader = StreamReader::new(TransferStream::new(
                        response.bytes_stream().map_err(std::io::Error::other),
                        info.id,
                        0,
                        current,
                        self.backend.events().clone(),
                    ));

                    let mut file = fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(false)
                        .open(info.destination)
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
                    id: info.id,
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
                    id: info.id,
                    block: 0,
                    failed: result.is_err(),
                })
                .ok();
        }

        result
    }

    /// Downloads a file in blocks using ranged GET requests.
    ///
    /// The blocks are downloaded in parallel and retried individually.
    async fn download_in_blocks(
        self: &Arc<Self>,
        info: DownloadInfo<'_>,
        content_length: u64,
        etag: &str,
        cancel: CancellationToken,
    ) -> Result<()> {
        // Calculate the block size and the number of blocks to download
        let block_size = self.backend.block_size(content_length)?;
        let num_blocks = content_length.div_ceil(block_size);

        debug!(
            "file `{source}` is {content_length} bytes and will be downloaded with {num_blocks} \
             block(s) of size {block_size}",
            source = info.source.display()
        );

        let file = std::fs::File::create(info.destination)
            .map_err(|error| Error::CreateTempFile { error })?;

        file.set_len(content_length).map_err(Error::from)?;

        let etag = Arc::new(etag.to_string());
        let file = Arc::new(file);
        let mut stream = stream::iter(0..num_blocks)
            .map(|block| {
                let id = info.id;
                let inner = self.clone();
                let file = file.clone();
                let source = info.source.clone();
                let etag = etag.clone();
                let cancel = cancel.clone();

                tokio::spawn(async move {
                    Retry::spawn_notify(
                        inner.backend.config().retry_durations(),
                        || {
                            inner
                                .download_block(
                                    source.clone(),
                                    BlockDownloadInfo {
                                        id,
                                        etag: etag.clone(),
                                        file: file.clone(),
                                        block,
                                        buffer: inner.pool.alloc(block_size as usize),
                                        range: (block * block_size)
                                            ..((block + 1) * block_size).min(content_length),
                                    },
                                    cancel.clone(),
                                )
                                .map_err(Error::into_retry_error)
                        },
                        notify_retry,
                    )
                    .await
                })
                .map(|r| r.expect("task panicked"))
            })
            .buffer_unordered(self.backend.config().parallelism());

        loop {
            let result = stream.next().await;
            match result {
                Some(r) => r?,
                None => break,
            }
        }

        Ok(())
    }

    /// Downloads a block of a file using a ranged GET request.
    async fn download_block(
        &self,
        source: Url,
        mut info: BlockDownloadInfo,
        cancel: CancellationToken,
    ) -> Result<()> {
        let id = info.id;
        let block = info.block;
        let start = info.range.start;
        let block_size = info.range.end - start;

        let transfer = async {
            let response = self
                .backend
                .get_range(
                    source,
                    info.etag.as_str(),
                    info.range.start,
                    Some(info.range.end),
                )
                .await?;

            // We expect partial content, otherwise treat as remote content modified
            if response.status() != StatusCode::PARTIAL_CONTENT {
                return Err(Error::RemoteContentModified);
            }

            let mut reader = StreamReader::new(TransferStream::new(
                response.bytes_stream().map_err(std::io::Error::other),
                info.id,
                info.block,
                0,
                self.backend.events().clone(),
            ));

            // Read the block into the provided buffer
            reader
                .read_exact(&mut info.buffer[0..block_size as usize])
                .map_err(Error::from)
                .await?;

            // Write the block to the file
            spawn_blocking(move || {
                crate::sys::write_at(&info.file, &info.buffer[0..block_size as usize], start)
                    .map_err(Error::from)
            })
            .await
            .expect("failed to join blocking task")?;

            Ok(())
        };

        if let Some(events) = self.backend.events() {
            events
                .send(TransferEvent::BlockStarted {
                    id,
                    block,
                    size: Some(block_size),
                })
                .ok();
        }

        let result = select! {
            biased;
            _ = cancel.cancelled() => Err(Error::Canceled),
            r = transfer => r,
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
        debug!(
            "using a maximum of {parallelism} parallel requests for file transfers",
            parallelism = backend.config().parallelism()
        );

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
                    r = self.inner.backend.walk(source.clone(), false) => r
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

        // If there are no files relative to the given URL, download just the given URL
        if paths.is_empty() {
            return self
                .inner
                .download(source, destination, self.cancel.clone())
                .await;
        }

        // Otherwise, download each file in turn
        for path in paths {
            let mut source = source.clone();
            let mut destination = destination.to_path_buf();

            // Adjust source and destination based on the provided relative URL path
            {
                let mut segments = source.path_segments_mut().expect("URL should have a path");
                segments.pop_if_empty();
                for segment in path.split('/') {
                    segments.push(segment);
                    destination.push(segment);
                }
            }

            self.inner
                .download(source, &destination, self.cancel.clone())
                .await?;
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
