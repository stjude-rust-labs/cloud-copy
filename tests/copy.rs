//! Tests for `cloud_copy::copy`.

use std::fs;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use cloud_copy::Alphanumeric;
use cloud_copy::AzureConfig;
use cloud_copy::Config;
use cloud_copy::Error;
use cloud_copy::HttpClient;
use cloud_copy::Location;
use cloud_copy::S3Config;
use cloud_copy::TransferEvent;
use cloud_copy::rewrite_url;
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use pretty_assertions::assert_eq;
use rand::Rng;
use tempfile::NamedTempFile;
use tempfile::tempdir;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::BufWriter;
use tokio::sync::broadcast;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use url::Url;
use walkdir::WalkDir;

/// Represents 1 MiB (in bytes).
const ONE_MEBIBYTE: usize = 1024 * 1024;

/// The test container/bucket name.
const TEST_BUCKET_NAME: &str = "cloud-copy-test";

/// Writes random bytes of the given size to the given file.
async fn write_random_bytes(file: File, mut size: usize) -> Result<()> {
    let mut rng = rand::rng();
    let mut buffer = [0; 4096];

    let mut writer = BufWriter::new(file);
    while size > 0 {
        let to_write = size.min(buffer.len());
        let buffer = &mut buffer[..to_write];
        rng.fill(buffer);
        size -= writer
            .write(buffer)
            .await
            .context("failed to write random file")?;
    }

    writer.flush().await.expect("failed to flush file");
    Ok(())
}

/// Determines if two files have the same content.
async fn same_file_content(first: &Path, second: &Path) -> Result<bool> {
    if first
        .metadata()
        .context("failed to read metadata of first file")?
        .len()
        != second
            .metadata()
            .context("failed to read metadata of second file")?
            .len()
    {
        return Ok(false);
    }

    let mut first = BufReader::new(
        File::open(first)
            .await
            .context("failed to open first file for comparison")?,
    );
    let mut second = BufReader::new(
        File::open(second)
            .await
            .context("failed to open second file for comparison")?,
    );

    let mut first_buffer = [0; 4096];
    let mut second_buffer = [0; 4096];

    loop {
        let read = first
            .read(&mut first_buffer)
            .await
            .context("failed to read first file for comparison")?;
        if read == 0 {
            break;
        }

        second
            .read_exact(&mut second_buffer[0..read])
            .await
            .context("failed to read first file for comparison")?;

        if first_buffer != second_buffer {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Creates random files in a directory.
fn create_random_files<'a>(dir: &'a Path, depth: usize) -> LocalBoxFuture<'a, Result<()>> {
    async move {
        let mut rng = rand::rng();

        for i in 0..5 {
            let file = File::create(dir.join(format!("file-{i}")))
                .await
                .context("failed to create test file")?;
            let size = { rng.random_range(0..ONE_MEBIBYTE) };
            write_random_bytes(file, size).await?;
        }

        if depth > 0 {
            let subdir = dir.join(format!("dir-{depth}"));
            fs::create_dir(&subdir).context("failed to create test directory")?;
            create_random_files(&subdir, depth - 1).await?;
        }

        Ok(())
    }
    .boxed_local()
}

/// Walks a directory and returns a sorted list of its entries.
///
/// The returns paths are relative to the given directory.
fn walk_dir(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut entries = Vec::new();

    for entry in WalkDir::new(dir) {
        let entry = entry.context("failed to walk directory")?;
        entries.push(
            entry
                .path()
                .strip_prefix(dir)
                .context("failed to strip directory prefix")?
                .to_path_buf(),
        );
    }

    entries.sort();
    Ok(entries)
}

/// Gets a set of cloud storage URLs to test given the test name.
fn urls(test: &str) -> Vec<Url> {
    vec![
        // S3 URLs
        format!("s3://{TEST_BUCKET_NAME}/1/{test}").parse().unwrap(),
        format!("http://s3.us-east-1.localhost.localstack.cloud:4566/{TEST_BUCKET_NAME}/2/{test}")
            .parse()
            .unwrap(),
        format!("http://{TEST_BUCKET_NAME}.s3.us-east-1.localhost.localstack.cloud:4566/3/{test}")
            .parse()
            .unwrap(),

        // Azure URLs
        format!("az://devstoreaccount1/{TEST_BUCKET_NAME}/1/{test}").parse().unwrap(),
        format!("http://devstoreaccount1.blob.core.windows.net.localhost:10000/{TEST_BUCKET_NAME}/2/{test}").parse().unwrap(),
    ]
}

fn config(overwrite: bool) -> Config {
    Config::builder()
        .with_overwrite(overwrite)
        .with_azure(AzureConfig::default().with_use_azurite(true).with_auth(
            "devstoreaccount1",
            // This is the Azurite secret key used for testing; it is not a secret
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/\
             KBHBeksoGMGw==",
        ))
        .with_s3(
            S3Config::default()
                .with_use_localstack(true)
                .with_auth("test", "test"),
        )
        .build()
}

/// Round trips a file of a given size by uploading it to cloud storage and then
/// downloading it again.
async fn roundtrip_file(test: &str, size: usize) -> Result<()> {
    let config = config(true);
    let client = HttpClient::default();
    let cancel = CancellationToken::new();

    // Create a new temp file of the specified size
    let (file, source) = NamedTempFile::new()
        .context("failed to create temp file")?
        .into_parts();
    write_random_bytes(file.into(), size)
        .await
        .context("failed to write random bytes into file")?;

    // Create another temporary file path for the download
    let destination = NamedTempFile::new()
        .context("failed to create temp file")?
        .into_temp_path();

    for url in urls(test) {
        // Copy the local file to the cloud
        cloud_copy::copy(
            config.clone(),
            client.clone(),
            &*source,
            &url,
            cancel.clone(),
            None,
        )
        .await
        .context("failed to upload file")?;

        // Copy the file from the cloud to a local file
        cloud_copy::copy(
            config.clone(),
            client.clone(),
            &url,
            &*destination,
            cancel.clone(),
            None,
        )
        .await
        .context("failed to download file")?;

        // Ensure the uploaded file and the downloaded file are the same
        if !same_file_content(&source, &destination)
            .await
            .context("failed to compare files")?
        {
            bail!("the downloaded file is not equal to the uploaded");
        }
    }

    Ok(())
}

/// A test to download a generic URL.
#[tokio::test]
async fn copy_generic_url() -> Result<()> {
    // Create a temporary file path for the download
    let destination = NamedTempFile::new()
        .context("failed to create temp file")?
        .into_temp_path();

    fs::remove_file(&destination).context("failed to delete destination file")?;

    // Copy the URL to the local file
    let cancel = CancellationToken::new();
    cloud_copy::copy(
        config(true),
        Default::default(),
        "https://example.com",
        &*destination,
        cancel.clone(),
        None,
    )
    .await
    .context("failed to download file")?;

    Ok(())
}

// A test to ensure existing destinations aren't overwritten.
#[tokio::test]
async fn no_overwrite() -> Result<()> {
    let test = format!("{random}", random = Alphanumeric::new(10));
    let config = config(true);
    let client = HttpClient::default();
    let cancel = CancellationToken::new();

    // Create an empty temp file
    let source = NamedTempFile::new()
        .context("failed to create temp file")?
        .into_temp_path();

    // Copy the local file to the cloud
    for url in urls(&test) {
        cloud_copy::copy(
            config.clone(),
            client.clone(),
            &*source,
            &url,
            cancel.clone(),
            None,
        )
        .await
        .context("failed to upload file")?;
    }

    let config = crate::config(false);

    // Attempt to overwrite the destination URLs
    for url in urls(&test) {
        match cloud_copy::copy(
            config.clone(),
            client.clone(),
            &*source,
            &url,
            cancel.clone(),
            None,
        )
        .await
        {
            Ok(_) => panic!("copy operation should fail for `{url}`"),
            Err(Error::RemoteDestinationExists(_)) => {}
            Err(e) => panic!("unexpected error `{e}` for `{url}`"),
        }
    }

    // Attempt to overwrite the source
    for url in urls(&test) {
        match cloud_copy::copy(
            config.clone(),
            client.clone(),
            &url,
            &*source,
            cancel.clone(),
            None,
        )
        .await
        {
            Ok(_) => panic!("copy operation should fail for `{url}`"),
            Err(Error::LocalDestinationExists(_)) => {}
            Err(e) => panic!("unexpected error `{e}` for `{url}`"),
        }
    }

    let config = crate::config(true);

    // Overwrite the destination URLs
    for url in urls(&test) {
        cloud_copy::copy(
            config.clone(),
            client.clone(),
            &*source,
            &url,
            cancel.clone(),
            None,
        )
        .await
        .context("failed to upload file")?;
    }

    // Overwrite the source
    for url in urls(&test) {
        cloud_copy::copy(
            config.clone(),
            client.clone(),
            &url,
            &*source,
            cancel.clone(),
            None,
        )
        .await
        .context("failed to download file")?;
    }

    Ok(())
}

/// A test to roundtrip an empty file.
#[tokio::test]
async fn roundtrip_empty_file() -> Result<()> {
    let test = format!("{random}", random = Alphanumeric::new(10));
    roundtrip_file(&test, 0).await
}

/// A test to roundtrip a small (1 byte to 10 MiB) file.
#[tokio::test]
async fn roundtrip_small_file() -> Result<()> {
    let test = format!("{random}", random = Alphanumeric::new(10));
    roundtrip_file(&test, rand::rng().random_range(1..10 * ONE_MEBIBYTE)).await
}

/// A test to roundtrip a medium (10 MiB to 50 MiB) file.
#[tokio::test]
async fn roundtrip_medium_file() -> Result<()> {
    let test = format!("{random}", random = Alphanumeric::new(10));
    roundtrip_file(
        &test,
        rand::rng().random_range(10 * ONE_MEBIBYTE..50 * ONE_MEBIBYTE),
    )
    .await
}

/// A test to roundtrip a large (50 MiB to 100 MiB) file.
#[tokio::test]
async fn roundtrip_large_file() -> Result<()> {
    let test = format!("{random}", random = Alphanumeric::new(10));
    roundtrip_file(
        &test,
        rand::rng().random_range(50 * ONE_MEBIBYTE..100 * ONE_MEBIBYTE),
    )
    .await
}

/// A test to roundtrip a directory.
#[tokio::test]
async fn roundtrip_directory() -> Result<()> {
    let test = format!("{random}", random = Alphanumeric::new(10));

    let source = tempdir().context("failed to create temporary directory")?;
    let destination = tempdir().context("failed to create temporary directory")?;

    create_random_files(source.path(), 3)
        .await
        .context("failed to populate temporary directory")?;

    let config = config(true);
    let client = HttpClient::default();
    let cancel = CancellationToken::new();
    for url in urls(&test) {
        // Copy the local directory to the cloud
        cloud_copy::copy(
            config.clone(),
            client.clone(),
            source.path(),
            &url,
            cancel.clone(),
            None,
        )
        .await
        .context("failed to upload directory")?;

        // Copy the directory from the cloud to a local directory (delete it first in
        // case it exists)
        fs::remove_dir_all(destination.path()).context("failed to delete destination directory")?;
        cloud_copy::copy(
            config.clone(),
            client.clone(),
            &url,
            destination.path(),
            cancel.clone(),
            None,
        )
        .await
        .context("failed to download directory")?;

        let source_entries = walk_dir(source.path()).context("failed to walk source directory")?;
        let destination_entries =
            walk_dir(destination.path()).context("failed to walk destination directory")?;

        if source_entries.len() != destination_entries.len() {
            bail!("source directory and download directory have a different number of entries");
        }

        for (orig, new) in source_entries.into_iter().zip(destination_entries) {
            if orig != new {
                bail!(
                    "original entry `{orig}` does not match the new entry `{new}`",
                    orig = orig.display(),
                    new = new.display()
                );
            }

            let source = source.path().join(orig);
            let destination = destination.path().join(new);

            match (source.is_dir(), destination.is_dir()) {
                (true, true) => continue,
                (false, false) => {
                    // Ensure the uploaded file and the downloaded file are the same
                    if !same_file_content(&source, &destination)
                        .await
                        .context("failed to compare files")?
                    {
                        bail!(
                            "contents of uploaded file `{source}` does not match the contents of \
                             downloaded file `{destination}`",
                            source = source.display(),
                            destination = destination.display()
                        );
                    }
                }
                _ => bail!(
                    "source entry `{source}` does not match the type of download entry \
                     `{destination}`",
                    source = source.display(),
                    destination = destination.display()
                ),
            }
        }
    }

    Ok(())
}

/// Test for linking to cached files.
///
/// This test runs only on Unix operating systems as the requisite [method on
/// Windows][1] is only available on nightly.
///
/// [1]: https://doc.rust-lang.org/std/os/windows/fs/trait.MetadataExt.html#tymethod.number_of_links
#[cfg(unix)]
#[tokio::test]
async fn link_to_cache() -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    let cache_dir = tempdir().context("failed to create temp directory")?;

    let destination = NamedTempFile::new()
        .context("failed to create destination file")?
        .into_temp_path();
    fs::remove_file(&destination).context("failed to delete destination file")?;

    let cancel = CancellationToken::new();

    let config = Config::default();
    let client = HttpClient::new_with_cache(config.clone(), &cache_dir);

    // Download the file (and cache it)
    cloud_copy::copy(
        config,
        client.clone(),
        "https://example.com",
        &*destination,
        cancel.clone(),
        None,
    )
    .await
    .context("failed to download file")?;

    assert!(destination.is_file(), "destination is not a file");

    fs::remove_file(&destination).context("failed to delete destination file")?;

    // Download the file again (it'll copy from the cache)
    cloud_copy::copy(
        Default::default(),
        client.clone(),
        "https://example.com",
        &*destination,
        cancel.clone(),
        None,
    )
    .await
    .context("failed to download file")?;

    assert_eq!(
        destination
            .metadata()
            .context("failed to read metadata of destination")?
            .nlink(),
        1,
        "expected only a single link to the file"
    );

    // Download the file again (it'll link from the cache)
    cloud_copy::copy(
        Config::builder()
            .with_overwrite(true)
            .with_link_to_cache(true)
            .build(),
        client,
        "https://example.com",
        &*destination,
        cancel,
        None,
    )
    .await
    .context("failed to download file")?;

    assert_eq!(
        destination
            .metadata()
            .context("failed to read metadata of destination")?
            .nlink(),
        2,
        "expected two links to the file"
    );

    Ok(())
}

/// Tests that transfer events are sent as expected.
#[tokio::test]
async fn events() -> Result<()> {
    const FILE_SIZE: u64 = 1024;

    let test = format!("{random}", random = Alphanumeric::new(10));

    let config = config(true);
    let client = HttpClient::default();
    let cancel = CancellationToken::new();

    // Create a new temp file
    let (file, source) = NamedTempFile::new()
        .context("failed to create temp file")?
        .into_parts();
    write_random_bytes(file.into(), FILE_SIZE as usize)
        .await
        .context("failed to write random bytes into file")?;

    // Create another temporary file path for the download
    let destination = NamedTempFile::new()
        .context("failed to create temp file")?
        .into_temp_path();

    let (events_tx, mut events_rx) = broadcast::channel(1000);
    let (urls_tx, urls_rx) = oneshot::channel();

    tokio::spawn(async move {
        let mut urls = Vec::new();
        loop {
            match events_rx.recv().await {
                Ok(event) => match event {
                    TransferEvent::TransferStarted {
                        source,
                        destination,
                        blocks,
                        size,
                        ..
                    } => {
                        assert_eq!(blocks, 1, "unexpected number of blocks");
                        assert_eq!(size, Some(FILE_SIZE), "unexpected file size");

                        if let Location::Url(url) = source {
                            urls.push(url);
                        }

                        if let Location::Url(url) = destination {
                            urls.push(url);
                        }
                    }
                    TransferEvent::BlockStarted { block, size, .. } => {
                        assert_eq!(block, 0, "unexpected block id");
                        assert_eq!(size, Some(FILE_SIZE), "unexpected file size");
                    }
                    TransferEvent::BlockProgress { .. } => continue,
                    TransferEvent::BlockCompleted { block, failed, .. } => {
                        assert_eq!(block, 0, "unexpected block id");
                        assert!(!failed, "block failed to transfer");
                    }
                    TransferEvent::TransferCompleted { failed, .. } => {
                        assert!(!failed, "file failed to transfer");
                    }
                },
                Err(broadcast::error::RecvError::Closed) => break,
                Err(broadcast::error::RecvError::Lagged(_)) => panic!("event channel lagged"),
            }
        }

        urls_tx.send(urls).expect("failed to send urls");
    });

    let expected = urls(&test);
    for url in &expected {
        // Copy the local file to the cloud
        cloud_copy::copy(
            config.clone(),
            client.clone(),
            &*source,
            url,
            cancel.clone(),
            Some(events_tx.clone()),
        )
        .await
        .context("failed to upload file")?;

        // Copy the file from the cloud to a local file
        cloud_copy::copy(
            config.clone(),
            client.clone(),
            url,
            &*destination,
            cancel.clone(),
            Some(events_tx.clone()),
        )
        .await
        .context("failed to download file")?;

        // Ensure the uploaded file and the downloaded file are the same
        if !same_file_content(&source, &destination)
            .await
            .context("failed to compare files")?
        {
            bail!("the downloaded file is not equal to the uploaded");
        }
    }

    drop(events_tx);

    // The returned URLs should have two entries per expected
    let urls = urls_rx.await.expect("failed to receive events");
    assert!(!urls.is_empty());
    for (i, expected) in expected.iter().enumerate() {
        let expected = rewrite_url(&config, expected).expect("URL should rewrite");
        assert_eq!(&urls[i * 2], expected.as_ref(), "unexpected upload URL");
        assert_eq!(
            &urls[(i * 2) + 1],
            expected.as_ref(),
            "unexpected download URL"
        );
    }

    Ok(())
}

/// Tests that we can walk cloud URLs.
#[tokio::test]
async fn walk() -> Result<()> {
    const FILE_SIZE: u64 = 1024;

    let test = format!("{random}", random = Alphanumeric::new(10));

    let config = config(true);
    let client = HttpClient::default();
    let cancel = CancellationToken::new();

    // Create a new temp file
    let (file, source) = NamedTempFile::new()
        .context("failed to create temp file")?
        .into_parts();
    write_random_bytes(file.into(), FILE_SIZE as usize)
        .await
        .context("failed to write random bytes into file")?;

    for url in urls(&test) {
        for i in 0..10 {
            let mut url = url.clone();
            url.path_segments_mut().unwrap().push(&i.to_string());

            // Copy the local file to the cloud
            cloud_copy::copy(
                config.clone(),
                client.clone(),
                &*source,
                url,
                cancel.clone(),
                None,
            )
            .await
            .context("failed to upload file")?;
        }

        let files = cloud_copy::walk(config.clone(), client.clone(), url.clone())
            .await
            .expect("should walk");
        assert_eq!(
            files,
            &["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"],
            "unexpected walk output for URL `{url}`"
        );
    }

    Ok(())
}
