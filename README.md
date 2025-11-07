<img style="margin: 0px" alt="Repository Header Image"
src="./assets/repo-header.png" />

<hr/>

<p align="center">
  <p align="center">
    <a href="https://github.com/stjude-rust-labs/cloud-copy/actions/workflows/CI.yml" target="_blank">
      <img alt="CI: Status" src="https://github.com/stjude-rust-labs/cloud-copy/actions/workflows/CI.yml/badge.svg" />
    </a>
    <a href="https://crates.io/crates/cloud-copy" target="_blank">
      <img alt="crates.io version" src="https://img.shields.io/crates/v/cloud-copy">
    </a>
    <img alt="crates.io downloads" src="https://img.shields.io/crates/d/cloud-copy">
    <a href="https://github.com/stjude-rust-labs/cloud-copy/blob/main/LICENSE-APACHE" target="_blank">
      <img alt="License: Apache 2.0" src="https://img.shields.io/badge/license-Apache 2.0-blue.svg" />
    </a>
    <a href="https://github.com/stjude-rust-labs/cloud-copy/blob/main/LICENSE-MIT" target="_blank">
      <img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-blue.svg" />
    </a>
  </p>

  <p align="center">
    A library and CLI tool for copying files to and from cloud storage.
    <br />
    <br />
    <a href="https://github.com/stjude-rust-labs/cloud-copy/issues/new?assignees=&title=Descriptive%20Title&labels=enhancement">Request Feature</a>
    ·
    <a href="https://github.com/stjude-rust-labs/cloud-copy/issues/new?assignees=&title=Descriptive%20Title&labels=bug">Report Bug</a>
    ·
    ⭐ Consider starring the repo! ⭐
    <br />
  </p>
</p>

## 📚 About `cloud-copy`

`cloud-copy` is a library and command line tool for copying files to and from
cloud storage.

### Supported Cloud Services

In addition to the supported cloud services below, `cloud-copy` supports
unauthenticated downloads over HTTP.

#### Azure Blob Storage

Supported remote URLs for [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs):

* `az` schemed URLs in the format `az://<account>/<container>/<blob>`.
* `https` schemed URLs in the format `https://<account>.blob.core.windows.net/<container>/<blob>`.

If authentication is required, the following environment variables may be set
when invoking `cloud-copy`:

* `AZURE_ACCOUNT_NAME` - the Azure Storage account name to use for
  authentication.
* `AZURE_ACCESS_KEY` - the Azure Storage account key to use for authentication.

#### AWS S3

Supported remote URLs for [S3 Storage](https://aws.amazon.com/s3/):

* `s3` schemed URLs in the format: `s3://<bucket>/<object>` (note: uses the
  default region).
* `https` schemed URLs in the format `https://<bucket>.s3.<region>.amazonaws.com/<object>`.
* `https` schemed URLs in the format `https://<region>.s3.amazonaws.com/<bucket>/<object>`.

If authentication is required, the following environment variables may be set
when invoking `cloud-copy`:

* `AWS_ACCESS_KEY_ID` - the AWS access key ID to use for authentication.
* `AWS_SECRET_ACCESS_KEY` - the AWS secret access key to use for authentication.
* `AWS_DEFAULT_REGION` - the default region to use for any `s3://` URLs.

#### Google Cloud Storage

Supported remote URLs for [Google Cloud Storage](https://cloud.google.com/storage):

* `gs` schemed URLs in the format: `gs://<bucket>/<object>`.
* `https` schemed URLs in the format `https://<bucket>.storage.googleapis.com/<object>`.
* `https` schemed URLs in the format `https://storage.googleapis.com/<bucket>/<object>`.

Note that [HMAC authentication](https://cloud.google.com/storage/docs/authentication/hmackeys) is used for Google Cloud Storage access.

If authentication is required, the following environment variables may be set
when invoking `cloud-copy`:

* `GOOGLE_HMAC_ACCESS_KEY` - the HMAC access key to use for authentication.
* `GOOGLE_HMAC_SECRET` - the HMAC secret to use for authentication.

## 🚀 Getting Started

### Installing Rust

Install a Rust toolchain via [`rustup`](https://rustup.rs/).

### Installing `cloud-copy`

To install `cloud-copy`, run the following command:

```bash
cargo install --features=cli cloud-copy
```

### Using `cloud-copy`

#### Uploading Files

A file or directory may be transferred to cloud storage by invoking
`cloud-copy`:

```bash
$ cloud-copy $SRC $DEST
```

Where `$SRC` is the path to the local file or directory to upload and `$DEST`
is a supported cloud storage service URL.

#### Downloading Files

A file or directory may be transferred from cloud storage by invoking
`cloud-copy`:

```bash
$ cloud-copy $SRC $DEST
```

Where `$SRC` is a supported cloud storage service URL and `$DEST` is the local
path to copy the file or directory to.

## 🧠 Running Automated Tests

Automated tests rely on having the following cloud service emulators installed:

* [Azurite](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio%2Cblob-storage)
  for Azure Blob Storage.
* [Localstack](https://github.com/localstack/localstack) for AWS S3.

Use the following command to run Azurite in a Docker container:

```bash
docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite azurite -l /data --blobHost 0.0.0.0 --loose
```

Use the following command to run Localstack in a Docker container:

```bash
docker run -p 4566:4566 localstack/localstack:s3-latest
```

The tests expect a container/bucket with the name `cloud-copy-test` to be
present.

To create the container with Azurite, use the Azure CLI:

```bash
az storage container create --name cloud-copy-test --connection-string "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1"
```

To create the bucket with Localstack, use the `awslocal` tool:

```bash
awslocal s3api create-bucket --bucket cloud-copy-test
```

Finally, run the tests:

```bash
cargo test --all
```

Note: the Azure tests expect `*.blob.core.windows.net.localhost` to resolve to
`127.0.0.1`.

## ✅ Submitting Pull Requests

Before submitting any pull requests, please make sure the code passes the
following checks (from the root directory).

```bash
# Run the project's tests.
cargo test --all-features

# Run the tests for the examples.
cargo test --examples --all-features

# Ensure the project doesn't have any linting warnings.
cargo clippy --all-features

# Ensure the project passes `cargo fmt`.
cargo fmt --check

# Ensure the docs build.
cargo doc --all-features
```

## 🤝 Contributing

Contributions, issues and feature requests are welcome! Feel free to check
[issues page](https://github.com/stjude-rust-labs/cloud-copy/issues).

## 📝 License

This project is licensed as either [Apache 2.0][license-apache] or
[MIT][license-mit] at your discretion. Additionally, please see [the
disclaimer](https://github.com/stjude-rust-labs#disclaimer) that applies to all
crates and command line tools made available by St. Jude Rust Labs.

Copyright © 2024-Present [St. Jude Children's Research
Hospital](https://github.com/stjude).

[license-apache]: https://github.com/stjude-rust-labs/cloud-copy/blob/main/LICENSE-APACHE
[license-mit]: https://github.com/stjude-rust-labs/cloud-copy/blob/main/LICENSE-MIT
