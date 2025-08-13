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
use cloud_copy::S3AuthConfig;
use cloud_copy::S3Config;
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use rand::Rng;
use tempfile::NamedTempFile;
use tempfile::tempdir;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::BufWriter;
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
        // These URLs use a SAS token that expires in 2050 and the default credentials of Azurite
        format!("az://devstoreaccount1/{TEST_BUCKET_NAME}/1/{test}?sv=2018-03-28&spr=https%2Chttp&st=2025-08-11T15%3A49%3A59Z&se=2050-08-11T15%3A49%3A00Z&sr=c&sp=rcwl&sig=0UK1EckCkj0k8Xi7s7yjB5QpjZa%2FVUmtd906SWAtO%2FM%3D").parse().unwrap(),
        format!("http://devstoreaccount1.blob.core.windows.net.localhost:10000/{TEST_BUCKET_NAME}/2/{test}?sv=2018-03-28&spr=https%2Chttp&st=2025-08-11T15%3A49%3A59Z&se=2050-08-11T15%3A49%3A00Z&sr=c&sp=rcwl&sig=0UK1EckCkj0k8Xi7s7yjB5QpjZa%2FVUmtd906SWAtO%2FM%3D").parse().unwrap(),
    ]
}

fn config() -> Config {
    Config {
        azure: AzureConfig { use_azurite: true },
        s3: S3Config {
            use_localstack: true,
            auth: Some(S3AuthConfig {
                access_key_id: "test".to_string(),
                secret_access_key: "test".into(),
            }),
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Round trips a file of a given size by uploading it to cloud storage and then
/// downloading it again.
async fn roundtrip_file(test: &str, size: usize) -> Result<()> {
    let config = config();
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
        cloud_copy::copy(config.clone(), &*source, url.clone(), cancel.clone(), None)
            .await
            .context("failed to upload file")?;

        // Copy the file from the cloud to a local file (delete it first in case it
        // exists)
        fs::remove_file(&destination).context("failed to delete destination file")?;
        cloud_copy::copy(
            config.clone(),
            url.clone(),
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
        config(),
        "https://example.com",
        &*destination,
        cancel.clone(),
        None,
    )
    .await
    .context("failed to download file")?;

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

    let config = config();
    let cancel = CancellationToken::new();
    for url in urls(&test) {
        // Copy the local directory to the cloud
        cloud_copy::copy(
            config.clone(),
            source.path(),
            url.clone(),
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
            url.clone(),
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
                            "contents of upo file `{source}` does not match the contents of \
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
